/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.akka.streaming

import com.typesafe.config.ConfigFactory
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

class AkkaSinkITCase extends StreamingMultipleProgramsTestBase {

  def datastream(implicit env: StreamExecutionEnvironment) = env.generateSequence(0, 20L)
  def job(f: StreamExecutionEnvironment => Unit) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    f(env)
    env.execute()
  }

  /**
    * JobExecutionException wraps the requirement
    *       "config.size must be equal to operator's parallelism"
    */
  @Test(expected = classOf[JobExecutionException])
  def checkExceptionConfigSize: Unit = job  { implicit env =>
    datastream.addSink(
      new AkkaSink[Long](path = "wrongPath", config = Seq(ConfigFactory.parseString("")))
    )
  }

  /**
    * JobExecutionException wraps
    *       "throw new IllegalArgumentException(s"Actor not found: $path", e)"
    */
  @Test(expected = classOf[JobExecutionException])
  def checkExceptionRemoteActorNotFound: Unit = job { implicit env =>
    env.setParallelism(1)

    datastream.addSink(
      new AkkaSink[Long](path = "wrongPath", config = Seq(ConfigFactory.parseString("")))
    )
  }
}


