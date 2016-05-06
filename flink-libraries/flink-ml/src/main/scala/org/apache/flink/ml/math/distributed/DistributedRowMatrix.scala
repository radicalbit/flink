package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.distributed.DistributedRowMatrix.MatrixRowIndex
import org.apache.flink.ml.math._


class DistributedRowMatrix(data: DataSet[IndexedRow], numRowsOpt: Option[Int] = None, numColsOpt: Option[Int] = None) extends DistributedMatrix {

  val numRows = numRowsOpt match {
    case Some(rows) => rows
    case None => data.count().toInt
  }
  val numCols =
    numColsOpt match {
      case Some(cols) => cols
      case None => getCols
    }

  val indices = data.map(_.rowIndex)
  val values = data.map(_.values)

  private def getCols: Int =
    data.first(1).collect().headOption match {
      case Some(vector) => vector.values.size
      case None => 0
    }

  def toCoo:Seq[(Int,Int,Double)]={
    val localRows = data.collect()
    for(
      IndexedRow(rowIndex,vector)<-localRows;

      (columnIndex,value)<-vector
    ) yield (rowIndex,columnIndex,value)

  }

  def toLocalSparseMatrix: SparseMatrix = {


    val localMatrix = SparseMatrix.fromCOO(this.numRows, this.numCols, this.toCoo)
    require(localMatrix.numRows == this.numRows)
    require(localMatrix.numCols == this.numCols)
    localMatrix
  }

  def toLocalDenseMatrix: DenseMatrix= this.toLocalSparseMatrix.toDenseMatrix
}

object DistributedRowMatrix {

  type MatrixRowIndex = Int

  def apply(data: DataSet[(Int, Int, Double)], numRows: Int, numCols: Int): DistributedRowMatrix
  = {
    val vectorData: DataSet[SparseVector] = data
      .groupBy(0)
      .reduceGroup(sparseRow => {
        val (indices, values) = sparseRow.map(x => (x._2, x._3)).toList.unzip
        SparseVector(numCols, indices.toArray, values.toArray)
      }
      )

    val zippedData=DataSetUtils(vectorData).zipWithIndex.map(x=>IndexedRow(x._1.toInt,x._2))

    new DistributedRowMatrix(zippedData, Some(numRows), Some(numCols))
  }
}

case class IndexedRow(rowIndex:Int,values:SparseVector)
