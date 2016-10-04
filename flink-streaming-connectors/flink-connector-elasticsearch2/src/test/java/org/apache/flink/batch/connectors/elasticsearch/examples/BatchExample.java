package org.apache.flink.batch.connectors.elasticsearch.examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.connectors.elasticsearch.ElasticsearchOutputFormat;
import org.apache.flink.batch.connectors.elasticsearch.ElasticsearchOutputFormatFunction;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class BatchExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		ArrayList<Tuple2<Integer, String>> collection = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple2<>(i, "string " + i));
		}

		DataSet<Tuple2<Integer, String>> dataSet = env.fromCollection(collection);

		List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
    	transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		dataSet.output(new ElasticsearchOutputFormat<Tuple2<Integer, String>>(
			new Properties(),
			transports,
			"test_index",
			"test_type",
			new ElasticsearchOutputFormatFunction<Tuple2<Integer, String>>() {
				@Override
				public Map createSource(Tuple2<Integer, String> element) {
					Map json = new HashMap<>();
					json.put("counter", element.f0);
					json.put("text", element.f1);
					return json;
				}
			}
		));

		env.execute("Write");
	}
}
