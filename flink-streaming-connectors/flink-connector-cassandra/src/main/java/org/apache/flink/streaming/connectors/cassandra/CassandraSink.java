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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Cluster.Builder;

public abstract class CassandraSink<IN extends Tuple> extends BaseCassandraSink<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);
	
	private static final long serialVersionUID = 1L;
	
	protected String query;
	protected PreparedStatement ps;
	
	
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
	public abstract Builder clusterBuilder(Builder cluster);

	@Override
	public void invoke(IN value) throws Exception {
		BoundStatement bd = ps.bind(extract(value));
		try {
			session.execute(bd);
		} catch (Exception e) {
			throw new IOException("writeRecord() failed",e);
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