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
import com.datastax.driver.core.Row;

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

			@Override
			public Tuple2<Integer, String> mapRow(Row item) {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(item.getInt(0), item.getString(1));
			}
		});
		
		try {
			env.execute();
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
