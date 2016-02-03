package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;

public abstract class CassandraMapperSink<IN> 
	extends BaseCassandraSink<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraMapperSink.class);

	private static final long serialVersionUID = 1L;

	private Class<IN> clazz;
	private Mapper<IN> mapper;

	public CassandraMapperSink(Class<IN> clazz) {
		this(null,clazz);
	}
	
	public CassandraMapperSink(String keyspace, Class<IN> clazz) {
		Preconditions.checkNotNull(clazz, "Clazz is not set");
		ClosureCleaner.ensureSerializable(clazz);
		this.keyspace = keyspace;
		this.clazz = clazz;
	}
	
	@Override
	public void open(Configuration configuration) {
		super.open(configuration);

		final MappingManager mappingManager = new MappingManager(session);
		this.mapper = mappingManager.mapper(clazz);
	}

	@Override
	public void invoke(IN value) throws Exception {
		mapper.save(value);
	}
}