package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public abstract class BaseCassandraSink<IN> extends RichSinkFunction<IN> implements ClusterBuilder {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraSink.class);
	
	protected transient Cluster cluster;
	protected transient Session session;
	
	protected String keyspace;

	@Override
	public void open(Configuration configuration) {
		
		this.cluster = clusterBuilder(Cluster.builder()).build();
		this.session = cluster.connect(keyspace);
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Cluster connection to Cassandra has been open. State: {} ",session.getState());
		}
	}
	@Override
	public abstract Builder clusterBuilder(Builder cluster);

	@Override
	public abstract void invoke(IN value) throws Exception;

	@Override
	public void close() {
		session.close();
		cluster.close();
	}
}