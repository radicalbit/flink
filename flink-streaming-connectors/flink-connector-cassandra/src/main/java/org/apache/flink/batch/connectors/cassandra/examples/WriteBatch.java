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

package org.apache.flink.batch.connectors.cassandra.examples;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;

import com.datastax.driver.core.Cluster.Builder;

public class WriteBatch {
	
	public static void main(String[] args) throws Exception {
		
		int insert = 20;
		if(args != null && args.length > 0 ){
			insert = Integer.parseInt(args[0]);
		}
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		
		ArrayList<Tuple2<Integer,String>> collection = new ArrayList<>(insert);
		for (int i = 0; i < insert; i++) {
			collection.add(new Tuple2<Integer, String>(i, "stringerz " +i));
		}
		
		DataSet<Tuple2<Integer, String>> dataset = environment.fromCollection(collection);
		
		dataset.output(new CassandraOutputFormat<Tuple2<Integer,String>>("test", "INSERT INTO writebatch (numberz, stringerz) VALUES (?, ?)") {

			@Override
			public Builder configureCluster(Builder cluster) {
				return cluster
						.addContactPoint("127.0.0.1");
			}
		});
		
		environment.execute("Write Batch Cassandra");
	}
}