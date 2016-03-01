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


import com.typesafe.config.ConfigFactory
import org.apache.flink.connectors.akka.streaming.AkkaSink
import org.apache.flink.streaming.api.scala._

object AkkaSinkExample extends Conf {

  val actorReceiverPath = "akka.tcp://actor-test@127.0.0.1:4000/user/receiver"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.generateSequence(0, 1000L).map(x => x.toString)
    stream.addSink(new AkkaSink[String]("test", actorReceiverPath, Seq(conf(5000))))

    env.execute("AkkaSinkExample")
  }
}

abstract class Conf {

  def conf(port: Int) = ConfigFactory.parseString {
    s"""
       |akka {
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |  remote {
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = $port
       |    }
       | }
       |}
       |""".stripMargin
  }
}
