package org.apache.flink.streaming.connectors.cassandra;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

public abstract class CassandraOutputFormat<OUT extends Tuple> 
	extends RichOutputFormat<OUT> implements ClusterBuilder {

	private static final long serialVersionUID = 1L;

	private transient Cluster cluster;
	private transient Session session;

	private String keyspace;
	private String query;

	protected PreparedStatement ps;
	
	public CassandraOutputFormat(String query) {
		this(null,query);
	}

	public CassandraOutputFormat(String keyspace, String query) {
		Preconditions.checkNotNull(query, "");
		this.keyspace = keyspace;
		this.query = query;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = clusterBuilder(Cluster.builder()).build();
		this.ps = session.prepare(query);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.session = cluster.connect(keyspace);
	}

	@Override
	public void writeRecord(OUT record) throws IOException {
		BoundStatement bd = ps.bind(extract(record));
		try {
			session.execute(bd);
		} catch (Exception e) {
			throw new IOException("writeRecord() failed",e);
		}
	}

	private Object[] extract(OUT record) {
		Object[] al = new Object[record.getArity()];
		for (int i = 0; i < record.getArity(); i++) {
			al[i] = (Object) record.getField(i);
		}
		return al;
	}

	@Override
	public void close() throws IOException {
		session.close();
		cluster.close();
	}
}