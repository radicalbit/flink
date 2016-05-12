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
import org.apache.flink.ml.math.SparseMatrix
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BlockMatrixTest extends FlatSpec with Matchers with GivenWhenThen {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val rawSampleData = List(
    (0, 0, 3.0),
    (1, 0, 1.0),
    (0, 1, 4.0)

  )

  val bm1 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData), 2, 2).toBlockMatrix(1, 1)

  val rawSampleData2 = List(
    (0, 0, 2.0),
    (1, 1, 1.0)
  )

  val bm2 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData2), 2, 2).toBlockMatrix(1, 1)


  "multiply" should "correctly multiply two matrices" in {
    bm1.multiply(bm2).getDataset.map(x => (x._1, x._2.toBreeze)).collect.foreach(println)
  }

  "sum" should "correctly sum two matrices" in {

    val rawSampleSum1 = List(
      (0, 0, 1.0),
      (7, 4, 3.0),
      (0, 1, 8.0),
      (2, 8, 12.0)
    )

    val rawSampleSum2 = List(
      (0, 0, 2.0),
      (3, 4, 4.0),
      (2, 8, 8.0)
    )

    val sumBlockMatrix1 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleSum1), 10, 10).toBlockMatrix(5, 5)
    val sumBlockMatrix2 = DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleSum2), 10, 10).toBlockMatrix(5, 5)

    val result = SparseMatrix.fromCOO(10, 10,
      List(
        (0, 0, 3.0),
        (0, 1, 8.0),
        (3, 4, 4.0),
        (2, 8, 20.0),
        (7, 4, 3.0)
      )
    )
    sumBlockMatrix1.sum(sumBlockMatrix2).toRowMatrix.toLocalDenseMatrix shouldBe result.toDenseMatrix

  }

}

