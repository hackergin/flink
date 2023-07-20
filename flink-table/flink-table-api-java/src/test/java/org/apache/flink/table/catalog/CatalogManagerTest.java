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
import org.apache.flink.table.catalog.listener.AlterDatabaseEvent;
import org.apache.flink.table.catalog.listener.CatalogModificationEvent;
import org.apache.flink.table.catalog.listener.CatalogModificationListener;
import org.apache.flink.table.catalog.listener.CreateDatabaseEvent;
import org.apache.flink.table.catalog.listener.DropDatabaseEvent;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CatalogManager}. */
class CatalogManagerTest {
    @Test
    void testDatabaseModificationEvent() throws Exception {
        CompletableFuture<CreateDatabaseEvent> createFuture = new CompletableFuture<>();
        CompletableFuture<AlterDatabaseEvent> alterFuture = new CompletableFuture<>();
        CompletableFuture<DropDatabaseEvent> dropFuture = new CompletableFuture<>();
        CatalogManager catalogManager =
                createCatalogManager(
                        new TestingCatalogModificationListener(
                                createFuture, alterFuture, dropFuture));

        // Validate create a database
        catalogManager.createDatabase(
                catalogManager.getCurrentCatalog(),
                "database1",
                new CatalogDatabaseImpl(
                        Collections.singletonMap("key1", "val1"), "database1 comment"),
                true);
        CreateDatabaseEvent createDatabaseEvent = createFuture.get(10, TimeUnit.SECONDS);
        assertThat(createDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(createDatabaseEvent.ignoreIfExists()).isTrue();
        assertThat(createDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThat(createDatabaseEvent.database().getComment()).isEqualTo("database1 comment");
        assertThat(createDatabaseEvent.database().getProperties())
                .isEqualTo(Collections.singletonMap("key1", "val1"));
        assertThat(alterFuture.isDone()).isFalse();
        assertThat(dropFuture.isDone()).isFalse();

        // Validate alter a database
        catalogManager.alterDatabase(
                catalogManager.getCurrentCatalog(),
                "database1",
                new CatalogDatabaseImpl(
                        Collections.singletonMap("key1", "val_val1"), "database1 comment modified"),
                false);
        AlterDatabaseEvent alterDatabaseEvent = alterFuture.get(10, TimeUnit.SECONDS);
        assertThat(alterDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(alterDatabaseEvent.ignoreIfNotExists()).isFalse();
        assertThat(alterDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThatThrownBy(alterDatabaseEvent::database)
                .hasMessage(
                        "There is no database in AlterDatabaseEvent, use database name instead.");
        assertThat(alterDatabaseEvent.newDatabase().getComment())
                .isEqualTo("database1 comment modified");
        assertThat(alterDatabaseEvent.newDatabase().getProperties())
                .isEqualTo(Collections.singletonMap("key1", "val_val1"));

        // Validate drop a database
        catalogManager.dropDatabase(catalogManager.getCurrentCatalog(), "database1", true, true);
        DropDatabaseEvent dropDatabaseEvent = dropFuture.get(10, TimeUnit.SECONDS);
        assertThat(dropDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(dropDatabaseEvent.ignoreIfNotExists()).isTrue();
        assertThat(dropDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThatThrownBy(dropDatabaseEvent::database)
                .hasMessage(
                        "There is no database in DropDatabaseEvent, use database name instead.");
        assertThat(dropDatabaseEvent.cascade()).isTrue();
    }

    private CatalogManager createCatalogManager(CatalogModificationListener listener) {
        return CatalogManager.newBuilder()
                .classLoader(CatalogManagerTest.class.getClassLoader())
                .config(new Configuration())
                .defaultCatalog("default", new GenericInMemoryCatalog("default"))
                .catalogModificationListeners(Collections.singletonList(listener))
                .catalogStore(new GenericInMemoryCatalogStore())
                .build();
    }

    /** Testing catalog modification listener. */
    static class TestingCatalogModificationListener implements CatalogModificationListener {
        private final CompletableFuture<CreateDatabaseEvent> createFuture;
        private final CompletableFuture<AlterDatabaseEvent> alterFuture;
        private final CompletableFuture<DropDatabaseEvent> dropFuture;

        TestingCatalogModificationListener(
                CompletableFuture<CreateDatabaseEvent> createFuture,
                CompletableFuture<AlterDatabaseEvent> alterFuture,
                CompletableFuture<DropDatabaseEvent> dropFuture) {
            this.createFuture = createFuture;
            this.alterFuture = alterFuture;
            this.dropFuture = dropFuture;
        }

        @Override
        public void onEvent(CatalogModificationEvent event) {
            if (event instanceof CreateDatabaseEvent) {
                createFuture.complete((CreateDatabaseEvent) event);
            } else if (event instanceof AlterDatabaseEvent) {
                alterFuture.complete((AlterDatabaseEvent) event);
            } else if (event instanceof DropDatabaseEvent) {
                dropFuture.complete((DropDatabaseEvent) event);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Test
    void testCatalogStore() {
        CatalogStore catalogStore = new GenericInMemoryCatalogStore();

        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");

        assertThatThrownBy(
                        () ->
                                catalogStore.storeCatalog(
                                        "cat1", CatalogDescriptor.of("cat1", configuration)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");

        CatalogManager catalogManager = CatalogManagerMocks.createCatalogManager(catalogStore);

        catalogManager.createCatalog("cat1", CatalogDescriptor.of("cat1", configuration));
        catalogManager.createCatalog("cat2", CatalogDescriptor.of("cat2", configuration));
        catalogManager.createCatalog("cat3", CatalogDescriptor.of("cat3", configuration));

        assertTrue(catalogManager.getCatalog("cat1").isPresent());
        assertTrue(catalogManager.getCatalog("cat2").isPresent());
        assertTrue(catalogManager.getCatalog("cat3").isPresent());

        assertTrue(catalogManager.listCatalogs().contains("cat1"));
        assertTrue(catalogManager.listCatalogs().contains("cat2"));
        assertTrue(catalogManager.listCatalogs().contains("cat3"));

        catalogManager.registerCatalog("cat4", new GenericInMemoryCatalog("cat4"));

        assertThatThrownBy(
                        () ->
                                catalogManager.createCatalog(
                                        "cat1", CatalogDescriptor.of("cat1", configuration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog cat1 already exists in catalog store.");

        assertThatThrownBy(
                        () ->
                                catalogManager.createCatalog(
                                        "cat4", CatalogDescriptor.of("cat4", configuration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog cat4 already exists in initialized catalogs.");

        catalogManager.unregisterCatalog("cat1", false);
        catalogManager.unregisterCatalog("cat2", false);
        catalogManager.unregisterCatalog("cat3", false);

        assertFalse(catalogManager.listCatalogs().contains("cat1"));
        assertFalse(catalogManager.listCatalogs().contains("cat2"));
        assertFalse(catalogManager.listCatalogs().contains("cat3"));

        catalogManager.close();

        assertThatThrownBy(() -> catalogManager.listCatalogs())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");
    }
}
