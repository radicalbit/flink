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
import org.scalatest.{Matchers, FlatSpec}

class DistributedRowMatrixTest extends FlatSpec with Matchers {

  behavior of "Flink's DistributedRowMatrix fromSortedCOO"

  val rawSampleData = List(
      (0, 0, 3.0),
      (0, 1, 3.0),
      (0, 3, 4.0),
      (2, 3, 4.0),
      (1, 4, 3.0),
      (1, 1, 3.0),
      (2, 1, 3.0),
      (2, 2, 3.0)
  )

  it should "contain the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset = env.fromCollection(rawSampleData)

    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toCOO.toSet.filter(_._3 != 0) shouldBe rawSampleData.toSet
  }

  it should "return a sparse local matrix containing the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset = env.fromCollection(rawSampleData)

    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toLocalSparseMatrix.iterator.filter(_._3 != 0).toSet shouldBe rawSampleData.toSet
  }

  it should "return a dense local matrix containing the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset = env.fromCollection(rawSampleData)

    val dmatrix = DistributedRowMatrix.fromCOO(rowDataset, 3, 5)

    dmatrix.toLocalDenseMatrix.iterator.filter(_._3 != 0).toSet shouldBe rawSampleData.toSet
  }
}
