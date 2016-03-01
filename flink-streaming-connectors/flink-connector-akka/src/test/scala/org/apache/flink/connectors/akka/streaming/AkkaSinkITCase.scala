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

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.scalatest.time.Seconds

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{FiniteDuration}

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

  /*@Test
  def example(): Unit = job { implicit env =>

    env.setParallelism(1)

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
    case object Count
    class ActorReceiver() extends Actor {
      @volatile var count = 0

      override def receive: Receive = {
        case l: String =>
          count += 1

        case Count =>
              sender ! count
      }
    }

    val remote_system = ActorSystem("remote-system", conf(4000))

    val remoteRef = remote_system.actorOf(Props(new ActorReceiver), "receiver")

    val path = "akka.tcp://remote-system@127.0.0.1:4000/user/receiver"

    datastream.map(_.toString).addSink(
      new AkkaSink[String](path = path, config = Seq(conf(5000)))
    )

    env.execute()



    Thread.sleep(100000L)

    val fCount =(remoteRef.ask(Count)(Timeout(10L, TimeUnit.SECONDS))).mapTo[Int]

    val res = Await.result(fCount, FiniteDuration(10,TimeUnit.SECONDS))


    assert(21L == res)
  }*/
}


