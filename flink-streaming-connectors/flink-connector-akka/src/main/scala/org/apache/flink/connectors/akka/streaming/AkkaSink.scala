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

import java.io.IOException
import java.util.concurrent.TimeUnit

import akka.actor._
import com.typesafe.config.Config
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
  * Sink that emits its input elements to a remote actor
  *
  * @param systemName Actor System's name, default is "akka-sink"
  * @param path remote actor's path
  * @param config Sequence of Akka System configuration
  * @tparam IN Type of the elements emitted by this sink
  */
class AkkaSink[IN](systemName: String = "akka-sink", path: String, config: Seq[Config]) extends RichSinkFunction[IN] {

  @transient private var actorSystem: ActorSystem = _

  @transient private var handler: ActorRef = _

  /**
    * Starts underlying Actor System and ActorHandler to manage connection from Sink to remote Actor
    *
    * @param parameters The configuration containing the parameters attached to the contract.
    * @throws IllegalArgumentException number of configurations must be equal to function's parallelism
    *                                  remote Actor not found via path
    */
  @throws[IllegalStateException]
  override def open(parameters: Configuration): Unit = {

    require(
      config.size == getRuntimeContext.getNumberOfParallelSubtasks,
      "config.size must be equal to operator's parallelism"
    )

    val index = getRuntimeContext.getIndexOfThisSubtask
    val systemName = s"$systemName-$index"

    // hardcoded
    val timeout = new FiniteDuration(10, TimeUnit.SECONDS)

    actorSystem = ActorSystem(systemName, config(index))

    // check remote actor exists
    val remote = try {
      Await.result(actorSystem.actorSelection(path).resolveOne(timeout), timeout)
    } catch {
      case e: Throwable => throw new IllegalArgumentException(s"Actor not found: $path", e)
    }

    handler = actorSystem.actorOf(Props(new ActorHandler(remote)))

  }

  override def invoke(value: IN): Unit = {
    handler ! value
  }

  /**
    * Stops ActorHandler and shutdowns the Actor System
    */
  override def close(): Unit = {
    actorSystem.stop(handler)
    actorSystem.shutdown()
  }

  /**
    * Sends values to remote actor and watch it in case of unexpected termination
    *
    * @param remote ActorRef's remote actor
    */
  final class ActorHandler(remote: ActorRef) extends Actor {

    context.watch(remote)

    override def receive: Receive = {
      case Terminated(actorRef) if remote == actorRef =>
        throw new IOException(s"Actor ${remote.path} has been terminated")
        context.stop(self)
      case Terminated(actorRef) if self == actorRef =>
        context.unwatch(remote)
      case element => remote ! element
    }
  }
}
