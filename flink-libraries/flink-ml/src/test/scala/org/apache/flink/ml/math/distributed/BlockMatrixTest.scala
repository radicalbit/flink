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

package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala._
import org.scalatest.{GivenWhenThen, Matchers, FlatSpec}

class BlockMatrixTest extends FlatSpec with Matchers with GivenWhenThen{

  val env=ExecutionEnvironment.getExecutionEnvironment

  val rawSampleData = List(
    (0, 0, 3.0),
    (2, 1, 1.0),
    (3,1,2.0)
  )

  val bm1=DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData), 4, 2).toBlockMatrix(2,2)

  val rawSampleData2 = List(
    (1, 0, 10.0),
    (0, 1, 1.0),
    (1,4,4.0)
  )

  val bm2=DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData2), 2, 4).toBlockMatrix(2,2)


  "multiply" should "correctly multiply two matrices" in{
    bm1.multiply(bm2).getDataset.map(x=>(x._1,x._2.toBreeze)).collect.foreach(println)
  }

}

