/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.datastax.driver.core.Cluster;
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
		
		CassandraSink<Tuple2<Long, String>> sink = new CassandraSink<Tuple2<Long, String>>(KEYSPACE, INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				String hostIp = EmbeddedCassandraServerHelper.getHost();
				int port = EmbeddedCassandraServerHelper.getNativeTransportPort();
				return cluster.addContactPoints(hostIp).withPort(port);//.withSocketOptions(getSocketOptions());
			}
		};
		
		source.addSink(sink);
		
		env.execute();

		ResultSet rs =  cassandraCQLUnit.session.execute(SELECT_QUERY);
		Assert.assertEquals(rs.all().size(), COUNT);
	}

	@After
	public void clean() {
		cassandraCQLUnit.session.execute("DROP TABLE test.tuplesink;");
		cassandraCQLUnit.session.execute("DROP KEYSPACE test;");
	}
}