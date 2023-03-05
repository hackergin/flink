package org.apache.flink.table.catalog;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;


public interface CatalogStoreFactory extends Factory {

    default CatalogStore createCatalogStore() {
        throw new CatalogException("CatalogStore factories must implement createCatalogStore()");
    }
}
