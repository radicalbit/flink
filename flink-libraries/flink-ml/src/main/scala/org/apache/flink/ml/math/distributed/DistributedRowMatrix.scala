package org.apache.flink.ml.math.distributed

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.distributed.DistributedRowMatrix.MatrixRowIndex
import org.apache.flink.ml.math._


import scala.reflect.ClassTag

/**
 *
 * @param data
 * @param numRowsOpt If None, will be calculated from the DataSet.
 * @param numColsOpt If None, will be calculated from the DataSet.
 */
class DistributedRowMatrix(data: DataSet[IndexedRow], numRowsOpt: Option[Int] = None, numColsOpt: Option[Int] = None) extends DistributedMatrix {

  lazy val numRows = numRowsOpt match {
    case Some(rows) => rows
    case None => data.count().toInt
  }
  lazy val numCols =
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

    val localRows=data.collect()

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


  /**
   * Builds a DistributedRowMatrix from a dataset in COO
   * @param data
   * @param numRows
   * @param numCols
   * @param isSorted If false, sorts the row to properly build the matrix representation.
   *                 If already sorted, set this parameter to true to skip sorting.
   * @return
   */
  def fromCOO(data: DataSet[(Int, Int, Double)], numRows: Int, numCols: Int, isSorted:Boolean=false): DistributedRowMatrix
  = {
    val vectorData: DataSet[SparseVector] = data
      .groupBy(0)
      .reduceGroup(sparseRow => {

        val sortedRow=
          if(isSorted)
            sparseRow.toList
          else
            sparseRow.toList.sortBy(row=>row._2)

        val (indices, values) = sortedRow.map(x => (x._2, x._3)).toList.unzip
        SparseVector(numCols, indices.toArray, values.toArray)
      }
      )

    val zippedData=DataSetUtils(vectorData).zipWithIndex.map(x=>IndexedRow(x._1.toInt,x._2))

    new DistributedRowMatrix(zippedData, Some(numRows), Some(numCols))
  }
}

case class IndexedRow(rowIndex:Int,values:SparseVector)
