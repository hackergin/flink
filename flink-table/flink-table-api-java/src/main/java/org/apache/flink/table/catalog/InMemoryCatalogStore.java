package org.apache.flink.table.catalog;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InMemoryCatalogStore implements CatalogStore {

    Map<String, Map<String, String>> catalogs;

    public InMemoryCatalogStore() {
        catalogs = new HashMap<>();
    }

    @Override
    public void storeCatalog(String catalogName, Map<String, String> properties) {

    }

    @Override
    public Map<String, String> removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        return catalogs.remove(catalogName);
    }

    @Override
    public Optional<Map<String, String>> getCatalog(String catalogName) {
        return Optional.ofNullable(catalogs.get(catalogName));
    }

    @Override
    public Set<String> listCatalogs() {
        return catalogs.keySet();
    }
}
