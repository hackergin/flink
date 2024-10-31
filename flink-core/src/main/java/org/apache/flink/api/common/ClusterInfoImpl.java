package org.apache.flink.api.common;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of the ClusterInfo interface that stores a single cluster ID option.
 * <p>
 * This class provides a simple way to create a ClusterInfo with one key-value pair
 * representing the cluster ID option.
 * </p>
 */
public class ClusterInfoImpl implements ClusterInfo {

    /** The key for the cluster ID option. */
    private final String clusterIdKey;

    /** The value for the cluster ID option. */
    private final String clusterIdValue;

    /**
     * Constructs a new ClusterInfoImpl with the specified cluster ID option.
     *
     * @param clusterIdKey The key for the cluster ID option.
     * @param clusterIdValue The value for the cluster ID option.
     */
    public ClusterInfoImpl(String clusterIdKey, String clusterIdValue) {
        this.clusterIdKey = clusterIdKey;
        this.clusterIdValue = clusterIdValue;
    }

    /**
     * Returns a map containing the single cluster ID option.
     *
     * @return A map with one entry, where the key is the cluster ID option key
     *         and the value is the cluster ID option value.
     */
    @Override
    public Map<String, String> getOptions() {
        return Collections.singletonMap(clusterIdKey, clusterIdValue);
    }
}
