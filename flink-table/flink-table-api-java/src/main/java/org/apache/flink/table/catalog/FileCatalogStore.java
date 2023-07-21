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

package org.apache.flink.table.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CatalogStore} that stores all catalog configuration to a directory. Configuration of
 * every catalog will be saved into a single file. The file name will be {catalogName}.yaml by
 * default.
 */
public class FileCatalogStore extends AbstractCatalogStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileCatalogStore.class);

    private static final String FILE_EXTENSION = ".yaml";

    /** The directory path where catalog configurations will be stored. */
    private final String catalogStoreDirectory;

    /** The character set to use when reading and writing catalog files. */
    private final String charset;

    /** A map of catalog file names to their corresponding paths. */
    private final Map<String, Path> files = new ConcurrentHashMap<>();

    /** The YAML parser to use when reading and writing catalog files. */
    private final Yaml yaml = new Yaml();

    /**
     * Creates a new {@link FileCatalogStore} instance with the specified directory path.
     *
     * @param catalogStoreDirectory the directory path where catalog configurations will be stored
     */
    public FileCatalogStore(String catalogStoreDirectory, String charset) {
        this.catalogStoreDirectory = catalogStoreDirectory;
        this.charset = charset;
    }

    /**
     * Opens the catalog store and initializes the catalog file map.
     *
     * @throws CatalogException if the catalog store directory does not exist or if there is an
     *     error reading the directory
     */
    @Override
    public void open() throws CatalogException {
        super.open();

        try {
            File directoryFile = new File(catalogStoreDirectory);
            if (!directoryFile.isDirectory()) {
                throw new CatalogException("File catalog store only support local directory.");
            }
            Files.list(directoryFile.toPath())
                    .filter(file -> file.getFileName().toString().endsWith(FILE_EXTENSION))
                    .filter(Files::isRegularFile)
                    .forEach(
                            p ->
                                    files.put(
                                            p.getFileName().toString().replace(FILE_EXTENSION, ""),
                                            p));
        } catch (Throwable e) {
            throw new CatalogException("Failed to open file catalog store directory", e);
        }
    }

    /**
     * Stores the specified catalog in the catalog store.
     *
     * @param catalogName the name of the catalog
     * @param catalog the catalog descriptor to store
     * @throws CatalogException if the catalog store is not open or if there is an error storing the
     *     catalog
     */
    @Override
    public void storeCatalog(String catalogName, CatalogDescriptor catalog)
            throws CatalogException {
        checkOpenState();

        Path filePath = files.computeIfAbsent(catalogName, name -> getCatalogPath(catalogName));

        try {
            String yamlString = yaml.dumpAsMap(catalog.getConfiguration().toMap());
            File file = filePath.toFile();
            file.createNewFile();
            FileUtils.writeFile(file, yamlString, charset);
            LOG.info("Catalog %s configuration saved to file: {}", filePath);
        } catch (Throwable e) {
            throw new CatalogException(
                    "Failed to save catalog configuration to file: " + filePath, e);
        }
    }

    /**
     * Removes the specified catalog from the catalog store.
     *
     * @param catalogName the name of the catalog to remove
     * @param ignoreIfNotExists whether to ignore if the catalog does not exist in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error removing
     *     the catalog
     */
    @Override
    public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        checkOpenState();

        Path path = files.remove(catalogName);
        try {
            File file = path.toFile();
            if (!file.isFile()) {
                throw new CatalogException(
                        String.format(
                                "Catalog : %s 's store file %s is not a regular file",
                                catalogName, path.getFileName()));
            }
            Files.deleteIfExists(path);
        } catch (Throwable e) {
            throw new CatalogException("Failed to delete catalog file: " + path, e);
        }
    }

    /**
     * Returns the catalog descriptor for the specified catalog, if it exists in the catalog store.
     *
     * @param catalogName the name of the catalog to retrieve
     * @return an {@link Optional} containing the catalog descriptor, or an empty {@link Optional}
     *     if the catalog does not exist in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error retrieving
     *     the catalog
     */
    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
        checkOpenState();

        Path filePath = files.get(catalogName);
        if (filePath == null) {
            return Optional.empty();
        }

        try {
            String content = FileUtils.readFile(filePath.toFile(), charset);
            Map<String, String> options = yaml.load(content);
            return Optional.of(CatalogDescriptor.of(catalogName, Configuration.fromMap(options)));
        } catch (Throwable e) {
            throw new CatalogException(
                    "Failed to load catalog configuration from file: " + filePath, e);
        }
    }

    /**
     * Returns a set of all catalog names in the catalog store.
     *
     * @return a set of all catalog names in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error retrieving
     *     the list of catalog names
     */
    @Override
    public Set<String> listCatalogs() throws CatalogException {
        checkOpenState();

        return Collections.unmodifiableSet(files.keySet());
    }

    /**
     * Returns whether the specified catalog exists in the catalog store.
     *
     * @param catalogName the name of the catalog to check
     * @return {@code true} if the catalog exists in the catalog store, {@code false} otherwise
     * @throws CatalogException if the catalog store is not open or if there is an error checking
     *     for the catalog
     */
    @Override
    public boolean contains(String catalogName) throws CatalogException {
        checkOpenState();

        return files.containsKey(catalogName);
    }

    private Path getCatalogPath(String catalogName) {
        return Paths.get(catalogStoreDirectory, catalogName + FILE_EXTENSION);
    }
}
