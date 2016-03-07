---
title: "Akka Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 6
sub-nav-title: Akka
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This connector provides a Sink that sends data to an
[Akka](https://akka.io/) Actor. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-akka{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Elasticsearch Sink
The connector provides a Sink that can send data to a remote actor.

This code shows how to create a sink:

<div class="codetabs" markdown="1">
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[String] = ...
val actorReceiverPath = "akka.tcp://actor-test@127.0.0.1:4000/user/receiver"
val conf = ConfigFactory.parseString {
    s"""
       |akka {
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |  remote {
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = 5000
       |    }
       | }
       |}
       |""".stripMargin

stream.addSink(
  new AkkaSink[String](
    "system-sink",      // Actor System's name, default is "akka-sink"
    actorReceiverPath,  // ActorPath's remote actor
    Seq(                // Sequence of Akka System's configuration
        conf
      )
    )
  )
{% endhighlight %}
</div>
</div>

More about information about Akka can be found [here](https://akka.io).
