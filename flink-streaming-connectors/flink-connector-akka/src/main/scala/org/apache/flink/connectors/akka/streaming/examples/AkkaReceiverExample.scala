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

import akka.actor._
import scala.tools.nsc.io.File

object AkkaReceiverExample extends Conf {

	val filename = "/Users/andreasella/Desktop/ciao.txt"

	def main(args: Array[String]): Unit = {

		val actorSystem = ActorSystem.create("actor-test", conf(4000))
		val actor = actorSystem.actorOf(Props(new ActorReceiver(filename)), "receiver")

		val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
		val path = actor.path.toStringWithAddress(address)
		print(path)
	}

	class ActorReceiver(filename: String) extends Actor with ActorLogging {
		override def receive = {
			case l =>
				log.info(s"###### $l")
				println(s"######## $l")
				File(filename).appendAll(s"$l \n")
		}
	}
}
