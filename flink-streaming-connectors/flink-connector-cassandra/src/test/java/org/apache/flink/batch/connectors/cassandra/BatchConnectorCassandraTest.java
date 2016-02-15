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
package org.apache.flink.batch.connectors.cassandra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.*;

import com.datastax.driver.core.Cluster.Builder;
import org.junit.rules.TemporaryFolder;

public class BatchConnectorCassandraTest extends TestBaseUtils {


    private static EmbeddedCassandraService cassandra;
    private static Cluster.Builder clusterCassandra;
    private static Session session;
    private static ForkableFlinkMiniCluster cluster;
    private static final int DEFAULT_PARALLELISM = 2;


    //
    //  QUERY
    //
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE test";

    private static final String CREATE_TABLE = "CREATE TABLE test.batchz(number bigint PRIMARY KEY, stringz text);";
	private static final String INSERT_QUERY = "INSERT INTO test.batchz (number, stringz) VALUES (?,?);";
	private static final String SELECT_QUERY = "SELECT number, stringz FROM test.batchz;";
    private static final String DROP_TABLE = "DROP TABLE test.batchz;";

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
    public static void startCassandra() throws Exception {

        cluster = TestBaseUtils.startCluster(1, DEFAULT_PARALLELISM, false, false, true);
        TestStreamEnvironment.setAsContext(cluster, DEFAULT_PARALLELISM);

        //generate temporary files
        ClassLoader classLoader = BatchConnectorCassandraTest.class.getClassLoader();
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

        session.execute(CREATE_KEYSPACE);
    }
	
	@Test
	public void write(){
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		ArrayList<Tuple2<Integer,String>> collection = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>(i, "stringz " + i));
		}
		
		DataSet<Tuple2<Integer,String>> dataSet = env.fromCollection(collection); 
		
		dataSet.output(new CassandraOutputFormat<Tuple2<Integer,String>>(CREATE_TABLE,INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return clusterCassandra;
			}
		});
		
		DataSet<Tuple2<Integer,String>> inputDS = env.createInput(new CassandraInputFormat<Tuple2<Integer,String>>(SELECT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				return clusterCassandra;
			}
		});
		
		try {
            long count =inputDS.count();
			Assert.assertEquals(count, 20L);
            session.execute(DROP_TABLE);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    @AfterClass
    public static void afterClass() throws Exception {
        session.execute(DROP_KEYSPACE);
        cassandra.stop();

        TestStreamEnvironment.unsetAsContext();
        stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
    }
}