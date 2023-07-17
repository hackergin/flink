package org.apache.flink.table.catalog;

import org.apache.flink.util.Preconditions;

/** The AbstractCatalogStore class is an abstract base class for implementing a catalog store. */
public abstract class AbstractCatalogStore implements CatalogStore {

    private boolean isOpen;

    /** Opens the catalog store. */
    @Override
    public void open() {
        isOpen = true;
    }

    /** Closes the catalog store. */
    @Override
    public void close() {
        isOpen = false;
    }

    /**
     * Returns whether the catalog store is currently open.
     *
     * @return true if the store is open, false otherwise
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Checks whether the catalog store is currently open.
     *
     * @throws IllegalStateException if the store is closed
     */
    public void checkOpenState() {
        Preconditions.checkState(isOpen(), "Catalog store is not open.");
    }
}
