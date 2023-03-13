package org.apache.flink.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

public class FileCatalogStoreFactory implements CatalogStoreFactory {

    public static final String IDENTIFIER = "file";

    public static ConfigOption<String> PATH_OPTIONS =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path used to store catalog configuration.");

    public static ConfigOption<String> FORMAT_OPTIONS =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("xml")
                    .withDescription("Used to specify the format used for saving configurations.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH_OPTIONS);
        options.add(FORMAT_OPTIONS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public CatalogStore createCatalogStore(Context context) {
        String path = context.getConfiguration().get(PATH_OPTIONS);
        String format = context.getConfiguration().get(FORMAT_OPTIONS);
        return new FileCatalogStore(path, format);
    }
}
