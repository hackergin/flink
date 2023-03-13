package org.apache.flink.table.catalog;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;

public interface CatalogStoreFactory extends Factory {

    default CatalogStore createCatalogStore(Context context) {
        throw new CatalogException("CatalogStore factories must implement createCatalogStore()");
    }

    interface Context {
        ReadableConfig getConfiguration();
    }

    class DefaultContext implements Context {
        private ReadableConfig readableConfig;

        public DefaultContext(ReadableConfig readableConfig) {
            this.readableConfig = readableConfig;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return readableConfig;
        }
    }
}
