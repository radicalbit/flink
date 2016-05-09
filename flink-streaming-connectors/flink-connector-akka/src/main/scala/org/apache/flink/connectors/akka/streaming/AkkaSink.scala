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

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.util.NetUtils
import org.slf4j.LoggerFactory

import scala.concurrent.Await

/**
  * Sink that emits its input elements to a remote actor
  *
  * @param path remote actor's path
  * @tparam IN Type of the elements emitted by this sink
  */
class AkkaSink[IN](path: String)(implicit timeout: akka.util.Timeout)
  extends RichSinkFunction[IN] {

  @transient private var actorSystem: ActorSystem = _

  @transient private var handler: ActorRef = _

  @transient private var asyncException: Throwable = _

  /**
    * Starts Actor System and ActorHandler to manage connection between Sink and remote Actor
    *
    * @param parameters The configuration containing the parameters attached to the contract.
    * @throws IllegalArgumentException remote Actor not found via path
    */
  @throws[IllegalStateException]
  override def open(parameters: Configuration): Unit = {

    actorSystem = AkkaSink.actorSystem
    val guardian = AkkaSink.actorGuardian

    // check if remote actor exists
    val remote = try {
      Await.result(actorSystem.actorSelection(path).resolveOne, timeout.duration)
    } catch {
      case e: Throwable => throw new IllegalArgumentException(s"Remote actor not found: $path", e)
    }

    handler = Await.result((guardian ? AkkaSink.Handle(remote)).mapTo[ActorRef], timeout.duration)
  }

  @throws[IOException]
  override def invoke(value: IN): Unit = {

    if(asyncException != null) {
      throw new IOException(asyncException)
    }

    val response = Await.result((handler ? value).mapTo[Either[Throwable,Unit]], timeout.duration)
    response.fold(t => asyncException = t, u => ())
  }

  /**
    * Stops ActorHandler and shutdowns the Actor System
    */
  override def close(): Unit = {
    actorSystem.stop(handler)
    AkkaSink.tryStopActorSystem()
  }

}

/**
  *  Companion Object
  */
object AkkaSink {

	/**
    *  TaskManager's ActorSystem of the AkkaSink
    *  It must be one per TaskManager
    */
  private lazy val actorSystem: ActorSystem = ActorSystem("akka-sink", AkkaSink.conf)

  private lazy val actorGuardian: ActorRef = actorSystem.actorOf(Props(new ActorGuardian()))

  protected case class Handle(remote: ActorRef)
  protected case object HandlerAlive

  /**
    *   If ActorGuardian doesn't have more children alive, actorSystem is gonna shutdown
    */
  private def tryStopActorSystem()(implicit timeout: akka.util.Timeout): Unit = {

    // remaining ActorHandler
    val actorHandlerAlive: Int = Await.result((actorGuardian ? HandlerAlive).mapTo[Int], timeout.duration)
    if (actorHandlerAlive == 0) {
      actorSystem.shutdown()
    }
  }
  /**
    * Actor System's configuration
    *
    */
  val conf = ConfigFactory.parseString {
    s"""
       |akka {
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |  enabled-transports = ["akka.remote.netty.tcp"]
       |  remote {
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = ${NetUtils.getAvailablePort}
       |    }
       | }
       |}
       |""".stripMargin
  }

  //
  //    INTERNAL ACTORS
  //

  /**
    *  Actor Guardian is responsible to manage lifecycle of the TaskManager's ActorHandlers
    *  It must be one per TaskManager
    */
  private final class ActorGuardian extends Actor {
    override def receive: Receive = {
      case Handle(remote) =>
        val ref: ActorRef = context.actorOf(Props(new ActorHandler(remote)))
        sender() ! ref
      case HandlerAlive =>
        sender() ! context.children.size
    }
  }

  /**
    * Sends values to remote actor and watch it in case of unexpected termination
    *
    * @param remote ActorRef's remote actor
    */
  private final class ActorHandler(remote: ActorRef) extends Actor {
    private var e: Throwable = null
    context.watch(remote)

    override def receive: Receive = {
      case Terminated(actorRef) if remote == actorRef =>
        e = new Throwable(s"Actor ${remote.path} has been terminated")
      case Terminated(actorRef) if self == actorRef =>
        context.unwatch(remote)
      case element =>
        remote ! element
        sender ! Either.cond(e == null, (), e)
    }
  }
}
