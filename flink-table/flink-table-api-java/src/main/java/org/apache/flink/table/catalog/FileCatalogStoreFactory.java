package org.apache.flink.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

public class FileCatalogStoreFactory implements CatalogStoreFactory {

    public static ConfigOption<String> PATH_OPTIONS = ConfigOptions.key("path").stringType().noDefaultValue().withDescription("");
    @Override
    public String factoryIdentifier() {
        return "file";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH_OPTIONS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
