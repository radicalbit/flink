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

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Mapper;
import com.google.common.base.Preconditions;


/**
 * 	Flink Sink to save data into a Cassandra cluster using {@link Mapper}, which it uses Java annotation from {@link com.datastax.mapping}.
 * 	See example. {@link WriteCassandraMapperSink }
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class CassandraMapperSink<IN> 
	extends BaseCassandraSink<IN> {

	private static final long serialVersionUID = 1L;

	protected Class<IN> clazz;

	protected Mapper<IN> mapper;

	protected MappingManager mappingManager;

	public CassandraMapperSink(Class<IN> clazz) {
		this(null,clazz);
	}
	
	public CassandraMapperSink(String keyspace, Class<IN> clazz) {
		Preconditions.checkNotNull(clazz, "Clazz is not set");
		ClosureCleaner.ensureSerializable(clazz);
		this.keyspace = keyspace;
		this.clazz = clazz;
	}
	
	@Override
	public void open(Configuration configuration) {
		super.open(configuration);

		this.mappingManager = new MappingManager(session);
		this.mapper = mappingManager.mapper(clazz);
	}

	@Override
	public void invoke(IN value) throws IOException {
		try {
			mapper.save(value);
		} catch (Exception e) {
			logError(e.getMessage());
			throw new IOException("invoke() failed", e);
		}
	}
	
	public Mapper<IN> getMapper() {
		return this.mapper;
	}
	
	public MappingManager getMappingManager() {
		return this.mappingManager;
	}
}