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

import java.io.Serializable;

import org.apache.flink.configuration.Configuration;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Mapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Flink Sink to save data into a Cassandra cluster using {@link Mapper}, which
 * it uses annotations from {@link com.datastax.mapping}. See example.
 * {@link org.apache.flink.streaming.connectors.cassandra.examples.WriteCassandraMapperSink }
 *
 * @param <IN>
 *            Type of the elements emitted by this sink
 */
public abstract class CassandraMapperSink<IN extends Serializable> extends
		BaseCassandraSink<IN,Void> {

	private static final long serialVersionUID = 1L;

	protected Class<IN> clazz;

	protected transient Mapper<IN> mapper;

	protected transient MappingManager mappingManager;

	/**
	 * Constructor for creating a CassandraMapperSink
	 *
	 * Attention! Into CQL query must be present the keyspace to ensure a
	 * correct execution. i.e. INSERT INTO keyspace.table ..
	 * 
	 * @param clazz
	 *            Class<IN> instance
	 */
	public CassandraMapperSink(Class<IN> clazz) {
		this(null, clazz);
	}


	/**
	 * The main constructor for creating CassandraMapperSink
	 *
	 * @param clazz
	 *            Class<IN> instance
	 * @param options
	 *            configuration for saving data
	 */
	public CassandraMapperSink(String createQuery, Class<IN> clazz) {
		super(createQuery);
		Preconditions.checkNotNull(clazz, "clazz is not set");
		this.clazz = clazz;
	}

	@Override
	public void open(Configuration configuration) {
		super.open(configuration);
		try {
			this.mappingManager = new MappingManager(session);
			this.mapper = mappingManager.mapper(clazz);
		} catch (Exception e) {
			logError(e.getMessage());
			throw new RuntimeException(
					"Cannot create CassandraMapperSink with input: "
							+ clazz.getSimpleName(), e);
		}
	}

	@Override
	public ListenableFuture<Void> send(IN value) {
		return  mapper.saveAsync(value);
	}
}