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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 *	BaseCassandraSink is the common abstract class of {@link CassandraMapperSink} and {@link CassandraSink}.
 *
 *	The {@link Cluster} is built via {@link #ClusterConfigurator.configureCluster(Cluster.Builder cluster) configureCluster} 
 *	inherited by {@link ClusterConfigurator}
 *
 *	The {@link Session} is liable to maintain the connection between our Sink and the Cassandra Cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class BaseCassandraSink<IN> extends RichSinkFunction<IN> implements ClusterConfigurator {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraSink.class);

	/** Cassandra Cluster instance */
	protected transient Cluster cluster;

	/** Session to Cassandra */
	protected transient Session session; 	
	
	protected String keyspace;

	@Override
	public void open(Configuration configuration) {
		
		this.cluster = configureCluster(Cluster.builder()).build();
		this.session = cluster.connect(keyspace);
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Cluster connection to Cassandra has been open. State: {} ",session.getState());
		}
	}

	@Override
	public void close() {
		session.close();
		cluster.close();
	}

	protected void logError(String error){
		if(LOG.isErrorEnabled()){
			LOG.error(error);
		}
	}

	public Session getSession() {
		return this.session;
	}

	public Cluster getCluster() {
		return this.cluster;
	}
}