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
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class AkkaSink[IN](sinkName: String, path: String, config:Config*) extends RichSinkFunction[IN] {

  @transient private var actorSystem: ActorSystem = _
  @transient private var handler: ActorRef = _

  private var asyncException: Throwable = null

  override def open(parameters: Configuration): Unit = {
    val index = getRuntimeContext.getIndexOfThisSubtask
    val systemName = s"$sinkName-$index"

    actorSystem = ActorSystem.create(systemName,config(index))
    handler = actorSystem.actorOf(ActorHandler.props(ActorPath.fromString(path)))
  }

  override def invoke(value: IN): Unit = {
    if(asyncException != null){
      throw new IOException(asyncException)
    }
    handler ! value
  }

  override def close(): Unit = {
    actorSystem.shutdown()
  }

  object ActorHandler {
    def props(path: ActorPath) = Props(new ActorHandler(path))
  }

  /**
    * Actor to manage connection between SinkFunction and Remote Actor
    *
    * @param path ActorPath address to remote actor
    */
  final class ActorHandler(path: ActorPath) extends Actor with ActorLogging {

    private var ref: Option[ActorRef] = None

    // buffer to collect elements until ActorRef has returned.
    private val queue = mutable.Queue.empty[IN]

    val timeout = new FiniteDuration(10, TimeUnit.SECONDS)
    val actorSelection = context.actorSelection(path)
    actorSelection.resolveOne(timeout) onComplete {
      case Success(actorRef) =>
        ref =  Some(actorRef)
        context watch ref.get
      case Failure(e) =>
        asyncException = new Throwable(s"Actor: $path is not alive", e)
    }

    override def receive: Receive = {
      case Terminated(actoRef) if ref.isDefined =>
        asyncException = new Throwable(s"Actor: $path has been stopped")
      case element: IN if ref.isDefined =>
        queue += element
      case element: IN if ref.isEmpty =>
        emptyBuffer()
        actorSelection ! element
    }

   def emptyBuffer(): Unit = {
      while(queue.nonEmpty){
        ref.get ! queue.dequeue()
      }
   }
  }
}
