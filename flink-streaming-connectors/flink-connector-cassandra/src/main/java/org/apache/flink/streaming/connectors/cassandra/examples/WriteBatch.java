package org.apache.flink.streaming.connectors.cassandra.examples;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.cassandra.CassandraOutputFormat;

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
			public Builder clusterBuilder(Builder cluster) {
				return cluster
						.addContactPoint("127.0.0.1");
			}
		});
		
		environment.execute("Write Batch Cassandra");
	}
}