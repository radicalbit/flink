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
package org.apache.flink.connectors.akka.streaming.examples


import com.typesafe.config.{ConfigFactory, Config}
import org.apache.flink.connectors.akka.streaming.AkkaSink
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object AkkaSinkExample {

  val path = "akka.tcp://actor-test@127.0.0.1:2343/user/receiver"
  val filename = "/Users/andreasella/Desktop/ciao.txt"
  val conf = ConfigFactory.parseString {
    s"""
       |akka {
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |  remote {
       |    //enabled-transports = ["akka.remote.netty.tcp"]
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = 2433
       |    }
       | }
       |}
     """.stripMargin
  }

  def main(args: Array[String]) : Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val ds:DataStreamSource[java.lang.Long] = env.generateSequence(0L,1000L)
    ds.addSink(new AkkaSink("test",path,conf))

    env.execute("AkkaSinkExample")

  }
}
