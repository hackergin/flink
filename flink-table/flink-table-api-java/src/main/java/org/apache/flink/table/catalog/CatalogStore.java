package org.apache.flink.table.catalog;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Interfaces defining catalog-related behaviors
 */

public interface CatalogStore {

    /**
     * Store a catalog under the give name. The catalog name must be unique.
     * @param catalogName name under which to register the given catalog
     * @param properties catalog properties to store
     * @throws CatalogException if the registration of the catalog under the given name failed
     */
    void storeCatalog(String catalogName, Map<String, String> properties);

    /**
     * Unregisters a catalog under the given name. The catalog name must be existed.
     *
     * @param catalogName name under which to unregister the given catalog.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     * @throws CatalogException if the unregistration of the catalog under the given name failed
     */
    Map<String, String> removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException;

    /**
     * Gets a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     *
     * @return the requested catalog or empty if it does not exist
     */
    Optional<Map<String, String>> getCatalog(String catalogName);


    /**
     * Retrieves names of all registered catalogs.
     *
     * @return a set of names of registered catalogs
     */
    Set<String> listCatalogs();
}
