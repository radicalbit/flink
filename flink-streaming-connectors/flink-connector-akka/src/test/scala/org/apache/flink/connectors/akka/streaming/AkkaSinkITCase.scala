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

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.util.NetUtils
import org.junit.{After, Before, Test}

import scala.io.Source
import scala.tools.nsc.io._


class AkkaSinkITCase extends StreamingMultipleProgramsTestBase {

	private var actorSystem: ActorSystem = _
	private var actorRemoteAddress: String = _
	private var file: java.io.File = _

	@Before
	def startReceiver(): Unit = {

		file = java.io.File.createTempFile("outputformat",".txt")
    val port = NetUtils.getAvailablePort
		actorSystem = ActorSystem.create("actor-test", AkkaSink.conf(port))
		actorSystem.actorOf(Props(new ActorReceiver(file.getAbsolutePath)), "receiver")

		actorRemoteAddress = s"akka.tcp://actor-test@127.0.0.1:$port/user/receiver"

		class ActorReceiver(filename: String) extends Actor with ActorLogging {
			override def receive = {
				case l : String =>
					log.debug(s"### element $l")
					File(filename).appendAll(s"$l \n")
			}
		}
	}

	@Test
	def ITCase(): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		//  AkkaSink implicit timeout
		implicit val timeout = akka.util.Timeout(10L, TimeUnit.SECONDS)
		val datastream = env.generateSequence(0,1000L).map(_.toString)

		datastream.addSink(new AkkaSink[String](actorRemoteAddress))

		env.execute("AkkaOutputFormat")

		/** Sleep is good **/
		Thread.sleep(4000L)

		val source = Source.fromFile(file).getLines()
		org.junit.Assert.assertEquals(source.length, 1001)

		for(line <- source) {
			org.junit.Assert.assertTrue(0 >= line.toInt || line.toInt < 1001)
		}

	}


	@After
	def close(): Unit = {
	  if(file.exists()){
			file.delete()
		}
		actorSystem.shutdown()
	}
}
