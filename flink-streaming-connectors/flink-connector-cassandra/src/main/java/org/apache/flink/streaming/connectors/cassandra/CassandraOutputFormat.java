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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

public abstract class CassandraOutputFormat<OUT extends Tuple> 
	extends RichOutputFormat<OUT> implements ClusterConfigurator {

	private static final long serialVersionUID = 1L;
	
	private transient Cluster cluster;
	
	private transient Session session;

	private String keyspace;
	
	private String query;

	protected PreparedStatement ps;
	
	public CassandraOutputFormat(String query) {
		this(null,query);
	}

	public CassandraOutputFormat(String keyspace, String query) {
		Preconditions.checkNotNull(query, "");
		this.keyspace = keyspace;
		this.query = query;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = configureCluster(Cluster.builder()).build();
		this.ps = session.prepare(query);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.session = cluster.connect(keyspace);
	}

	@Override
	public void writeRecord(OUT record) throws IOException {
		BoundStatement bd = ps.bind(extract(record));
		try {
			session.execute(bd);
		} catch (Exception e) {
			throw new IOException("writeRecord() failed",e);
		}
	}

	private Object[] extract(OUT record) {
		Object[] al = new Object[record.getArity()];
		for (int i = 0; i < record.getArity(); i++) {
			al[i] = (Object) record.getField(i);
		}
		return al;
	}

	@Override
	public void close() throws IOException {
		session.close();
		cluster.close();
	}
}