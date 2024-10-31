package org.apache.flink.api.common;

import java.util.Collections;
import java.util.Map;

/**
 * Provides options to retrieve cluster information in both application and session modes.
 * <p>
 * This interface offers a standardized way to access cluster-specific configuration options,
 * which can be used to generate a unique cluster ID.
 * </p>
 *
 */
public interface ClusterInfo {

    /**
     * Retrieves the options necessary for cluster information.
     * <p>
     * These options can be used to generate a cluster ID using the
     * {@code org.apache.flink.client.deployment.ClusterClientFactory#getClusterId(Configuration)} method.
     * </p>
     *
     * @return A map containing key-value pairs of cluster information options.
     */
    Map<String, String> getOptions();

    /**
     * Creates a ClusterInfo instance with a single option.
     *
     * @param clusterIdKey The key for the cluster ID option.
     * @param clusterIdValue The value for the cluster ID option.
     * @return A new ClusterInfo instance containing the specified option.
     */
    static ClusterInfo of(String clusterIdKey, String clusterIdValue) {
        return new ClusterInfoImpl(clusterIdKey, clusterIdValue);
    }

    /**
     * Creates an empty ClusterInfo instance.
     *
     * @return A ClusterInfo instance with no options.
     */
    static ClusterInfo empty() {
        return Collections::emptyMap;
    }
}
