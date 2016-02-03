package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;

public class CassandraSinkTest extends StreamingMultipleProgramsTestBase {
	
	private static final long COUNT = 20;
	private static final String KEYSPACE = "test";
	private static final String SELECT_QUERY = "SELECT * FROM test.tuplesink;";
	private static final String INSERT_QUERY = "INSERT INTO tuplesink (id,value) VALUES (?,?);";

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
			new ClassPathCQLDataSet("script.cql",KEYSPACE));

	@Test
	public void write() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Long, String>> source = env
				.addSource(new SourceFunction<Tuple2<Long, String>>() {

					private boolean running = true;
					private volatile long cnt = 0;

					@Override
					public void run(SourceContext<Tuple2<Long, String>> ctx)
							throws Exception {
						while (running) {
							ctx.collect(new Tuple2<Long, String>(cnt,
									"cassandra-" + cnt));
							cnt++;
							if (cnt == COUNT) {
								cancel();
							}
						}
					}

					@Override
					public void cancel() {
						running = false;
					}
				});
		
		source.addSink(new CassandraSink<Tuple2<Long, String>>(KEYSPACE, INSERT_QUERY) {

			@Override
			public Builder clusterBuilder(Builder cluster) {
				String hostIp = EmbeddedCassandraServerHelper.getHost();
				int port = EmbeddedCassandraServerHelper.getNativeTransportPort();
				return cluster.addContactPoints(hostIp).withPort(port);
			}
		});
		
		env.execute();

		ResultSet rs = cassandraCQLUnit.session.execute(SELECT_QUERY);
		Assert.assertEquals(rs.all().size(), COUNT);
	}

	@After
	public void clean() {
		cassandraCQLUnit.session.execute("DROP TABLE test.tuplesink;");
		cassandraCQLUnit.session.execute("DROP KEYSPACE test;");
	}
}