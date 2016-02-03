package org.apache.flink.streaming.connectors.cassandra;

import java.io.IOException;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

public abstract class CassandraInputFormat<OUT extends Tuple, IN extends InputSplit>
	extends RichInputFormat<OUT, IN> implements ClusterBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraInputFormat.class);

	private static final long serialVersionUID = 1L;

	private String keyspace;
	private String query;

	private transient Cluster cluster;
	private transient Session session;
	
	private volatile ResultSet rs;

	public CassandraInputFormat(String query) {
		this(null, query);
	}

	public CassandraInputFormat(String keyspace, String query){
		Preconditions.checkNotNull(query, "Query not set");
		this.keyspace = keyspace;
		this.query = query;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = clusterBuilder(Cluster.builder()).build();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
			throws IOException {
		return cachedStatistics;
	}

	@Override
	public IN[] createInputSplits(int minNumSplits) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(IN[] inputSplits) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open(IN split) throws IOException {
		this.session = cluster.connect(keyspace);
		this.rs = session.execute(query);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		final Boolean res = rs.isExhausted();
		if (res) {
			this.close();
		}
		return res;
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		final Row item = rs.one();
		return mapRow(item);
	}

	public abstract OUT mapRow(Row item);

	@Override
	public void close() throws IOException {
		session.close();
		cluster.close();
	}
}