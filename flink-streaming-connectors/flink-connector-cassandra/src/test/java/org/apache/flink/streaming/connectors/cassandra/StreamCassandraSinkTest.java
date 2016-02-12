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

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import com.datastax.driver.core.Session;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import org.junit.rules.TemporaryFolder;

public class StreamCassandraSinkTest extends TestBaseUtils {

    private static EmbeddedCassandraService cassandra;
    private static Cluster.Builder clusterCassandra;
    private static Session session;

	private static final long COUNT = 20;

    //
    //  QUERY
    //
    private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
    private static final String CREATE_QUERY = "CREATE TABLE test.tuplesink(id bigint PRIMARY KEY, value text);";
	private static final String INSERT_QUERY = "INSERT INTO test.tuplesink (id,value) VALUES (?,?);";
    private static final String SELECT_QUERY = "SELECT * FROM test.tuplesink;";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE test";

    private static final String CREATE_QUERY_MAPPER = "CREATE TABLE test.mappersink(id bigint,value text,PRIMARY KEY(id))";
    private static final String SELECT_QUERY_MAPPER = "SELECT * FROM test.mappersink;";

    private static final int DEFAULT_PARALLELISM = 2;
    private static ForkableFlinkMiniCluster cluster;

    private static class EmbeddedCassandraService {
        CassandraDaemon cassandraDaemon;

        public void start() throws IOException {
            this.cassandraDaemon = new CassandraDaemon();
            this.cassandraDaemon.init(null);
            this.cassandraDaemon.start();
        }

        public void stop() {
            this.cassandraDaemon.stop();
        }
    }


    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception {
        cluster = TestBaseUtils.startCluster(1, DEFAULT_PARALLELISM, false, false, true);
        TestStreamEnvironment.setAsContext(cluster, DEFAULT_PARALLELISM);
    }

    @BeforeClass
    public static void startCassandra() throws IOException {
            //generate temporary files
            ClassLoader classLoader = StreamCassandraSinkTest.class.getClassLoader();
            File file = new File(classLoader.getResource("cassandra.yaml").getFile());
            File tmp = testFolder.newFile("cassandra.yaml");
            testFolder.newFolder("data");
            testFolder.newFolder("commit");
            testFolder.newFolder("cache");
            BufferedWriter b = new BufferedWriter(new FileWriter(tmp));

            //copy cassandra.yaml; inject absolute paths into cassandra.yaml
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    line = line.replace("$PATH", "'"  + tmp.getParentFile());
                    b.write(line  + "\n");
                    b.flush();
                }
            scanner.close();


            // Tell cassandra where the configuration files are.
            // Use the test configuration file.
            System.setProperty("cassandra.config", "file:" + File.separator + File.separator + File.separator + tmp.getAbsolutePath());

            cassandra = new EmbeddedCassandraService();
            cassandra.start();

            clusterCassandra = Cluster.builder().addContactPoint("127.0.0.1");
            session = clusterCassandra.build().connect();

            session.execute(CREATE_KEYSPACE_QUERY);
    }

	@Test(expected = IllegalArgumentException.class)
	public void queryNotSet() {
		new CassandraSink<Tuple2<Long, String>>(null) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return clusterCassandra;
			}
		};
	}

	@Test
	public void write() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Long, String>> source = env
				.addSource(new SourceFunction<Tuple2<Long, String>>() {

					private boolean running = true;
					private volatile long cnt = 0;

					@Override
					public void run(SourceContext<Tuple2<Long, String>> ctx)
							throws Exception {
						while (running) {
							ctx.collect(new Tuple2<>(cnt, "cassandra-" + cnt));
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

		
		source.addSink(new CassandraSink<Tuple2<Long, String>>(CREATE_QUERY, INSERT_QUERY) {

            @Override
            public Builder configureCluster(Builder cluster) {
                return clusterCassandra;
            }
        });
		
		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ResultSet rs =  session.execute(SELECT_QUERY);
		Assert.assertEquals(rs.all().size(), COUNT);
	}

	//
	// CassandraMapperSink.java
	//

	@Test(expected = NullPointerException.class)
	public void clazzNotSet() {

		class Foo implements Serializable {
		}
		new CassandraMapperSink<Foo>(null) {

			@Override
			public Builder configureCluster(Builder cluster) {
                return clusterCassandra;
			}
		};
	}

	@Test
	public void write2() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Pojo> source = env
                .addSource(new SourceFunction<Pojo>() {

                    private boolean running = true;
                    private volatile long cnt = 0;

                    @Override
                    public void run(SourceContext<Pojo> ctx) throws Exception {
                        while (running) {
                            ctx.collect(new Pojo(cnt, "cassandra-" + cnt));
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

        source.addSink(new CassandraMapperSink<Pojo>(CREATE_QUERY_MAPPER,Pojo.class) {

            @Override
            public Builder configureCluster(Builder cluster) {
                return clusterCassandra;
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        ResultSet rs = session.execute(SELECT_QUERY_MAPPER);
        Assert.assertEquals(rs.all().size(), COUNT);
    }

    @AfterClass
    public static void afterClass() {
        session.execute(DROP_KEYSPACE);
        cassandra.stop();
    }

    @AfterClass
    public static void teardown() throws Exception {
        TestStreamEnvironment.unsetAsContext();
        stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
    }
}