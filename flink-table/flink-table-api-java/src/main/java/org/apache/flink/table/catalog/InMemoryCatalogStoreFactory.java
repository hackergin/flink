package org.apache.flink.table.catalog;

import org.apache.flink.configuration.ConfigOption;

import java.util.HashSet;
import java.util.Set;

public class InMemoryCatalogStoreFactory implements CatalogStoreFactory {
    @Override
    public String factoryIdentifier() {
        return "in_memory";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public CatalogStore createCatalogStore(Context context) {
        return new FileCatalogStore();
    }
}
