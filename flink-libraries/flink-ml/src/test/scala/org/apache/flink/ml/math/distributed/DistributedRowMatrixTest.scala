package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.SparseVector
import org.scalatest.{Matchers, FlatSpec}

class DistributedRowMatrixTest extends FlatSpec with Matchers{

  behavior of "Flink's DistributedRowMatrix fromSortedCOO"

  val rawSampleData=List(
    (0,0,3.0),
    (0,1,3.0),
    (0,3,4.0),


    (2,3,4.0),

    (1,4,3.0),
    (1,1,3.0),

    (2,1,3.0),
    (2,2,3.0)
  )



  it should "contain the initialization data" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset=env.fromCollection(rawSampleData)

    val dmatrix=DistributedRowMatrix.fromCOO(rowDataset,3,5)


    dmatrix.toCoo.toSet.filter(_._3!=0) shouldBe  rawSampleData.toSet
  }



  it should "return a sparse local matrix containing the initialization data"  in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset=env.fromCollection(rawSampleData)

    val dmatrix=DistributedRowMatrix.fromCOO(rowDataset,3,5)


    dmatrix.toLocalSparseMatrix.iterator.filter(_._3!=0).toSet shouldBe rawSampleData.toSet
  }

  it should "return a dense local matrix containing the initialization data"  in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rowDataset=env.fromCollection(rawSampleData)

    val dmatrix=DistributedRowMatrix.fromCOO(rowDataset,3,5)


    dmatrix.toLocalDenseMatrix.iterator.filter(_._3!=0).toSet shouldBe rawSampleData.toSet
  }
}
