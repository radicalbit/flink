package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.distributed.DistributedRowMatrix
import org.scalatest.{Matchers, FlatSpec}

class MatrixConversionsTest extends FlatSpec with Matchers {

  "RowGroupReducer.calculateSlices" should "return correct slicing ranges" in{
    val env=ExecutionEnvironment.getExecutionEnvironment


    val rowGroupReducer1=new RowGroupReducer(3,3,8,8)

    rowGroupReducer1.calculateSlices() shouldBe Seq((0,2),(3,5),(6,7))

    val rowGroupReducer2=new RowGroupReducer(25,25,1000,1000)

    rowGroupReducer2.calculateSlices().size shouldBe 40

    val rowGroupReducer3=new RowGroupReducer(22,22,1000,1000)

    rowGroupReducer3.calculateSlices().last shouldBe (990,999)

  }
  "DistributedRowMatrix.toBlockMatrix" should "preserve the matrix structure after conversion" in {
    val env=ExecutionEnvironment.getExecutionEnvironment
    val rawSampleData=List(
      (0,0,3.0),
      (0,1,1.0),
      (0,3,4.0),
      (2,3,60.0),
      (1,2,50.0),
      (1,1,12.0),
      (2,1,14.0),
      (3,2,18.0)
    )


    val d1=DistributedRowMatrix.fromCOO(env.fromCollection(rawSampleData),4,4)
    val blockMatrix=d1.toBlockMatrix(2,2)
    blockMatrix.getDataset.collect().foreach(println)

  }

}
