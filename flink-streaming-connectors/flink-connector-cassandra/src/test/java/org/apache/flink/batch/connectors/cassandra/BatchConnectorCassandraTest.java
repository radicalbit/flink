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

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;

import com.datastax.driver.core.Cluster.Builder;

public class BatchConnectorCassandraTest {

	private static final String KEYSPACE = "test";
	private static final String INSERT_QUERY = "INSERT INTO batchz (number, stringz) VALUES (?,?);";
	private static final String SELECT_QUERY = "SELECT number, stringz FROM batchz;";

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
			new ClassPathCQLDataSet("script-batch.cql",KEYSPACE));
	
	@Test
	public void write(){
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		ArrayList<Tuple2<Integer,String>> collection = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<Integer, String>(i, "stringz " + i));
		}
		
		DataSet<Tuple2<Integer,String>> dataSet = env.fromCollection(collection); 
		
		dataSet.output(new CassandraOutputFormat<Tuple2<Integer,String>>(KEYSPACE, INSERT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				String hostIp = EmbeddedCassandraServerHelper.getHost();
				int port = EmbeddedCassandraServerHelper.getNativeTransportPort();
				return cluster.addContactPoints(hostIp).withPort(port);
			}
		});
		
		DataSet<Tuple2<Integer,String>> inputDS = env.createInput(new CassandraInputFormat<Tuple2<Integer,String>>(KEYSPACE, SELECT_QUERY) {

			@Override
			public Builder configureCluster(Builder cluster) {
				String hostIp = EmbeddedCassandraServerHelper.getHost();
				int port = EmbeddedCassandraServerHelper.getNativeTransportPort();
				return cluster.addContactPoints(hostIp).withPort(port);
			}
		});
		
		try {
			//env.execute();
			Assert.assertEquals(inputDS.count(), 20L);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@After
	public void after(){
		cassandraCQLUnit.session.close();
		cassandraCQLUnit.cluster.close();
	}
}
