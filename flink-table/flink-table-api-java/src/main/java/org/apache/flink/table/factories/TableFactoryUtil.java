/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.listener.CatalogModificationListener;
import org.apache.flink.table.catalog.listener.CatalogModificationListenerFactory;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility for dealing with {@link TableFactory} using the {@link TableFactoryService}. */
public class TableFactoryUtil {

    /** Returns a table source matching the descriptor. */
    @SuppressWarnings("unchecked")
    public static <T> TableSource<T> findAndCreateTableSource(TableSourceFactory.Context context) {
        try {
            return TableFactoryService.find(
                            TableSourceFactory.class, context.getTable().toProperties())
                    .createTableSource(context);
        } catch (Throwable t) {
            throw new TableException("findAndCreateTableSource failed.", t);
        }
    }

    /**
     * Creates a {@link TableSource} from a {@link CatalogTable}.
     *
     * <p>It considers {@link Catalog#getFactory()} if provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> TableSource<T> findAndCreateTableSource(
            @Nullable Catalog catalog,
            ObjectIdentifier objectIdentifier,
            CatalogTable catalogTable,
            ReadableConfig configuration,
            boolean isTemporary) {
        TableSourceFactory.Context context =
                new TableSourceFactoryContextImpl(
                        objectIdentifier, catalogTable, configuration, isTemporary);
        Optional<TableFactory> factoryOptional =
                catalog == null ? Optional.empty() : catalog.getTableFactory();
        if (factoryOptional.isPresent()) {
            TableFactory factory = factoryOptional.get();
            if (factory instanceof TableSourceFactory) {
                return ((TableSourceFactory<T>) factory).createTableSource(context);
            } else {
                throw new ValidationException(
                        "Cannot query a sink-only table. "
                                + "TableFactory provided by catalog must implement TableSourceFactory");
            }
        } else {
            return findAndCreateTableSource(context);
        }
    }

    /** Returns a table sink matching the context. */
    @SuppressWarnings("unchecked")
    public static <T> TableSink<T> findAndCreateTableSink(TableSinkFactory.Context context) {
        try {
            return TableFactoryService.find(
                            TableSinkFactory.class, context.getTable().toProperties())
                    .createTableSink(context);
        } catch (Throwable t) {
            throw new TableException("findAndCreateTableSink failed.", t);
        }
    }

    /**
     * Creates a {@link TableSink} from a {@link CatalogTable}.
     *
     * <p>It considers {@link Catalog#getFactory()} if provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> TableSink<T> findAndCreateTableSink(
            @Nullable Catalog catalog,
            ObjectIdentifier objectIdentifier,
            CatalogTable catalogTable,
            ReadableConfig configuration,
            boolean isStreamingMode,
            boolean isTemporary) {
        TableSinkFactory.Context context =
                new TableSinkFactoryContextImpl(
                        objectIdentifier,
                        catalogTable,
                        configuration,
                        !isStreamingMode,
                        isTemporary);
        if (catalog == null) {
            return findAndCreateTableSink(context);
        } else {
            return createTableSinkForCatalogTable(catalog, context)
                    .orElseGet(() -> findAndCreateTableSink(context));
        }
    }

    /**
     * Creates a table sink for a {@link CatalogTable} using table factory associated with the
     * catalog.
     */
    public static Optional<TableSink> createTableSinkForCatalogTable(
            Catalog catalog, TableSinkFactory.Context context) {
        TableFactory tableFactory = catalog.getTableFactory().orElse(null);
        if (tableFactory instanceof TableSinkFactory) {
            return Optional.ofNullable(((TableSinkFactory) tableFactory).createTableSink(context));
        }
        return Optional.empty();
    }

    /** Checks whether the {@link CatalogTable} uses legacy connector sink options. */
    public static boolean isLegacyConnectorOptions(
            @Nullable Catalog catalog,
            ReadableConfig configuration,
            boolean isStreamingMode,
            ObjectIdentifier objectIdentifier,
            CatalogTable catalogTable,
            boolean isTemporary) {
        // normalize option keys
        DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(catalogTable.getOptions());
        if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) {
            return true;
        } else {
            try {
                // try to create legacy table source using the options,
                // some legacy factories may use the 'type' key
                TableFactoryUtil.findAndCreateTableSink(
                        catalog,
                        objectIdentifier,
                        catalogTable,
                        configuration,
                        isStreamingMode,
                        isTemporary);
                // success, then we will use the legacy factories
                return true;
            } catch (Throwable ignore) {
                // fail, then we will use new factories
                return false;
            }
        }
    }

    /** Find and create modification listener list from configuration. */
    public static List<CatalogModificationListener> findCatalogModificationListenerList(
            final ReadableConfig configuration, final ClassLoader classLoader) {
        return configuration.getOptional(TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS)
                .orElse(Collections.emptyList()).stream()
                .map(
                        identifier ->
                                FactoryUtil.discoverFactory(
                                                classLoader,
                                                CatalogModificationListenerFactory.class,
                                                identifier)
                                        .createListener(
                                                new CatalogModificationListenerFactory.Context() {
                                                    @Override
                                                    public ReadableConfig getConfiguration() {
                                                        return configuration;
                                                    }

                                                    @Override
                                                    public ClassLoader getUserClassLoader() {
                                                        return classLoader;
                                                    }
                                                }))
                .collect(Collectors.toList());
    }

    /**
     * Finds and creates a {@link CatalogStore} using the provided {@link Configuration} and user
     * classloader.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.catalog-store.kind: {identifier}
     * table.catalog-store.{identifier}.{param1}: xxx
     * table.catalog-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    public static CatalogStore findAndCreateCatalogStore(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.getString(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);
        String catalogStoreOptionPrefix =
                CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX + identifier + ".";
        Map<String, String> options =
                new DelegatingConfiguration(configuration, catalogStoreOptionPrefix).toMap();
        CatalogStoreFactory catalogStoreFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogStoreFactory.class, identifier);
        CatalogStoreFactory.Context context =
                new FactoryUtil.DefaultCatalogStoreContext(options, configuration, classLoader);
        catalogStoreFactory.open(context);
        return CatalogStoreWithFactory.of(
                catalogStoreFactory.createCatalogStore(), catalogStoreFactory);
    }

    /**
     * A wrapper class for {@link CatalogStore} that includes a {@link CatalogStoreFactory}.
     *
     * <p>This class can be used by users to close both the {@link CatalogStore} and {@link
     * CatalogStoreFactory} instances.
     */
    private static class CatalogStoreWithFactory implements CatalogStore {

        private CatalogStore catalogStore;

        private CatalogStoreFactory factory;

        public static CatalogStore of(CatalogStore catalogStore, CatalogStoreFactory factory) {
            return new CatalogStoreWithFactory(catalogStore, factory);
        }

        private CatalogStoreWithFactory(CatalogStore catalogStore, CatalogStoreFactory factory) {
            this.catalogStore = catalogStore;
            this.factory = factory;
        }

        @Override
        public void storeCatalog(String catalogName, CatalogDescriptor catalog)
                throws CatalogException {
            catalogStore.storeCatalog(catalogName, catalog);
        }

        @Override
        public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
                throws CatalogException {
            catalogStore.removeCatalog(catalogName, ignoreIfNotExists);
        }

        @Override
        public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
            return catalogStore.getCatalog(catalogName);
        }

        @Override
        public Set<String> listCatalogs() throws CatalogException {
            return catalogStore.listCatalogs();
        }

        @Override
        public boolean contains(String catalogName) throws CatalogException {
            return catalogStore.contains(catalogName);
        }

        @Override
        public void open() throws CatalogException {
            catalogStore.open();
        }

        @Override
        public void close() throws CatalogException {
            catalogStore.close();
            factory.close();
        }
    }

    public static CatalogStore createCatalogStore(
            CatalogStoreFactory factory, Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.getString(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);
        String catalogStoreOptionPrefix =
                CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX + identifier + ".";
        Map<String, String> options =
                new DelegatingConfiguration(configuration, catalogStoreOptionPrefix).toMap();
        CatalogStoreFactory catalogStoreFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogStoreFactory.class, identifier);
        return catalogStoreFactory.createCatalogStore();
    }

    /**
     * Finds and creates a {@link CatalogStoreFactory} using the provided {@link Configuration} and
     * user classloader.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.catalog-store.kind: {identifier}
     * table.catalog-store.{identifier}.{param1}: xxx
     * table.catalog-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    public static CatalogStoreFactory findAndCreateCatalogStoreFactory(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.getString(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);

        CatalogStoreFactory catalogStoreFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogStoreFactory.class, identifier);

        return catalogStoreFactory;
    }

    public static CatalogStoreFactory.Context buildCatalogStoreFactoryContext(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.getString(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);
        String catalogStoreOptionPrefix =
                CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX + identifier + ".";
        Map<String, String> options =
                new DelegatingConfiguration(configuration, catalogStoreOptionPrefix).toMap();
        CatalogStoreFactory.Context context =
                new FactoryUtil.DefaultCatalogStoreContext(options, configuration, classLoader);

        return context;
    }
}
