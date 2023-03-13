package org.apache.flink.table.catalog;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class FileCatalogStore implements CatalogStore {

    private final String path;
    private final String format;

    public FileCatalogStore(String path, String format) {
        this.path = path;
        this.format = format;
    }

    @Override
    public void storeCatalog(String catalogName, Map<String, String> properties) {

    }

    @Override
    public Map<String, String> removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        return null;
    }

    @Override
    public Optional<Map<String, String>> getCatalog(String catalogName) {
        return Optional.empty();
    }

    @Override
    public Set<String> listCatalogs() {
        return null;
    }
}
