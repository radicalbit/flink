package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;

public interface ClusterBuilder {
	Cluster.Builder clusterBuilder(Cluster.Builder cluster);
}
