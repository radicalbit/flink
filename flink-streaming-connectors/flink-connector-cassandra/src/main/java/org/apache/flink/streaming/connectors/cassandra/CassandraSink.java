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

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

/**
 * Flink Sink to save data into a Cassandra cluster. 
 *
 * @param <IN> Type of the elements emitted by this sink, it must extend {@link Tuple}
 */
public abstract class CassandraSink<IN extends Tuple> extends BaseCassandraSink<IN> {
	
	private static final long serialVersionUID = 1L;
	
	/** CQL query */
	protected String query;
	
	/** CQL statement */
	protected PreparedStatement ps;
	
	/**
	 * Constructor for creating a CassandraSink
	 * 
	 * Attention! Into CQL query must be present the keyspace to ensure a correct execution.
	 * i.e. INSERT INTO keyspace.table ..
	 * @param query CQL query
	 */
	public CassandraSink(String query) {
		this(null, query);
	}
	
	/**
	 * The main constructor for creating a CassandraSink
	 * 
	 * @param keyspace Cassandra keyspace where the query will be executed.
	 * @param query	CQL query
	 */
	public CassandraSink(String keyspace, String query){
		this.keyspace = keyspace;
		this.query = query;
	}
	
	@Override
	public void open(Configuration configuration) {
		super.open(configuration);
		this.ps = session.prepare(query);
	}

	@Override
	public void invoke(IN value) throws Exception {
		BoundStatement bd = ps.bind(extract(value));
		try {
			session.execute(bd);
		} catch (Exception e) {
			logError(e.getMessage());
			throw new IOException("invoke() failed", e);
		}
	}

	private Object[] extract(IN record) {
		Object[] al = new Object[record.getArity()];
		for (int i = 0; i < record.getArity(); i++) {
			al[i] = (Object) record.getField(i);
		}
		return al;
	}
}