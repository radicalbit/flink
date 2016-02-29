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

class AkkaSink[IN](sinkName: String, path: String, config: Config*) extends RichSinkFunction[IN] {

  @transient private var actorSystem: ActorSystem = _

  @transient private var handler: ActorRef = _

  @throws[IllegalStateException]
  override def open(parameters: Configuration): Unit = {
    val index = getRuntimeContext.getIndexOfThisSubtask
    val systemName = s"$sinkName-$index"

    val timeout = new FiniteDuration(10, TimeUnit.SECONDS)
    actorSystem = ActorSystem(systemName, config(index))

    val remote = try {
      Await.result(actorSystem.actorSelection(path).resolveOne(timeout), timeout)
    } catch {
      case e: Throwable => throw new IllegalStateException(s"Actor not found: $path", e)
    }
     handler = actorSystem.actorOf(Props(new ActorHandler(remote)))
  }

  override def invoke(value: IN): Unit = {
    handler ! value
  }

  override def close(): Unit = {
    actorSystem.shutdown()
  }
}

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
