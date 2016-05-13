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

import java.lang

import breeze.linalg.{CSCMatrix => BreezeSparseMatrix, Matrix => BreezeMatrix, Vector => BreezeVector}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, _}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 *
 * @param numRowsOpt If None, will be calculated from the DataSet.
 * @param numColsOpt If None, will be calculated from the DataSet.
 */
class DistributedRowMatrix(data: DataSet[IndexedRow],
                           numRowsOpt: Option[Int] = None,
                           numColsOpt: Option[Int] = None)
  extends DistributedMatrix {

  lazy val getNumRows: Int = numRowsOpt match {
    case Some(rows) => rows
    case None => data.count().toInt
  }

  lazy val getNumCols: Int =
    numColsOpt match {
      case Some(cols) => cols
      case None => calcCols
    }


  val getRowData = data

  private def calcCols: Int =
    data.first(1).collect().headOption match {
      case Some(vector) => vector.values.size
      case None => 0
    }

  /**
   * Collects the data in the form of a sequence of coordinates associated with their values.
   * @return
   */
  def toCOO: Seq[(Int, Int, Double)] = {

    val localRows = data.collect()

    for (
      IndexedRow(rowIndex, vector) <- localRows;
      (columnIndex, value) <- vector
    ) yield (rowIndex, columnIndex, value)

  }

  /**
   * Collects the data in the form of a SparseMatrix
   * @return
   */
  def toLocalSparseMatrix: SparseMatrix = {
    val localMatrix = SparseMatrix.fromCOO(this.getNumRows, this.getNumCols, this.toCOO)
    require(localMatrix.numRows == this.getNumRows)
    require(localMatrix.numCols == this.getNumCols)
    localMatrix
  }

  //TODO: convert to dense representation on the distributed matrix and collect it afterward
  def toLocalDenseMatrix: DenseMatrix = this.toLocalSparseMatrix.toDenseMatrix


  def toBlockMatrix(rowsPerBlock: Int = 1024, colsPerBlock: Int = 1024): BlockMatrix = {
    require(rowsPerBlock > 0 && colsPerBlock > 0,
      "Block sizes must be a strictly positive value.")
    require(rowsPerBlock <= getNumRows && colsPerBlock <= getNumCols,
      "Blocks can't be bigger than the matrix")

    val blockMapper = BlockMapper(getNumRows, getNumCols, rowsPerBlock, colsPerBlock)

    val rowGroupReducer = new RowGroupReducer(blockMapper)

    val blocks = data
      .groupBy(row => blockMapper.absCoordToMappedCoord(row.rowIndex, 0)._1)
      .reduceGroup(rowGroupReducer)


    new BlockMatrix(blocks, blockMapper)
  }


}

object DistributedRowMatrix {

  type MatrixRowIndex = Int


  /**
   * Builds a DistributedRowMatrix from a dataset in COO
   * @param isSorted If false, sorts the row to properly build the matrix representation.
   *                 If already sorted, set this parameter to true to skip sorting.
   * @return
   */
  def fromCOO(data: DataSet[(Int, Int, Double)],
              numRows: Int,
              numCols: Int,
              isSorted: Boolean = false): DistributedRowMatrix
  = {
    val vectorData: DataSet[(Int, SparseVector)] = data
      .groupBy(0)
      .reduceGroup(sparseRow => {
        require(sparseRow.nonEmpty)
        val sortedRow =
          if (isSorted) {
            sparseRow.toList
          }
          else {
            sparseRow.toList.sortBy(row => row._2)
          }
        val (indices, values) = sortedRow.map(x => (x._2, x._3)).unzip
        (sortedRow.head._1, SparseVector(numCols, indices.toArray, values.toArray))
      }
      )

    val zippedData = vectorData
      .map(
        x =>
          IndexedRow(x._1.toInt, x._2))

    new DistributedRowMatrix(zippedData, Some(numRows), Some(numCols))
  }
}

case class IndexedRow(rowIndex: Int, values: Vector) extends Ordered[IndexedRow] {

  def compare(other: IndexedRow) = this.rowIndex.compare(other.rowIndex)

  override def toString: String = s"($rowIndex,${values.toString}"

}

/**
 * Serializable Reduction function used by the toBlockMatrix function. Takes an ordered list of
 * indexed row and split those rows to form blocks.
 */
class RowGroupReducer(blockMapper: BlockMapper)
  extends RichGroupReduceFunction[IndexedRow, (Int, Block)] {

  import org.apache.flink.ml.math.Breeze._

  override def reduce(values: lang.Iterable[IndexedRow], out: Collector[(Int, Block)]): Unit = {

    val sortedRows = values.toList.sorted
    require(sortedRows.max.rowIndex - sortedRows.min.rowIndex <= blockMapper.rowsPerBlock)
    val slicesWithIndex: List[((Int, Int), Int)] = calculateSlices().zipWithIndex

    val splitRows: List[(Int, Int, Vector)] =
      sortedRows
        .flatMap(
          row => {
            val breezeVector = row.values.asBreeze
            slicesWithIndex.map(
              sliceWithIndex => {
                val ((start, end), sliceIndex) = sliceWithIndex
                (row.rowIndex, sliceIndex, breezeVector(start to end).toVector.fromBreeze)
              })

          })

    splitRows.groupBy(_._2).map(splitRowGroup => {
      val (mappedColIndex, rowGroup) = splitRowGroup
      createBlock(rowGroup, blockMapper)
    })
      .foreach(blockWithIndices => {
        val (i, j, block) = blockWithIndices


        val blockID = blockMapper.getBlockIdByMappedCoord(i, j)
        out.collect((blockID, block))
      })


  }

  def calculateSlices(): List[(Int, Int)] =

    (0 to (math.ceil(blockMapper.numCols * 1.0 / blockMapper.colsPerBlock) - 1).toInt)
      .map(
        x => {

          val start = x * blockMapper.colsPerBlock

          val endT = (x + 1) * blockMapper.colsPerBlock - 1
          val end = if (endT > blockMapper.numCols - 1) {
            blockMapper.numCols - 1
          }
          else {
            endT
          }
          (start, end)
        }
      ).toList


  //Create a Block with mapped coordinates from an intermediate unstructured block
  def createBlock(data: List[(Int, Int, Vector)], blockMapper: BlockMapper): (Int, Int, Block) = {
    require(data.nonEmpty)
    val mappedColIndex = data.head._2
    val mappedRowIndex = blockMapper.absCoordToMappedCoord(data.head._1, 0)._1
    val coo = data.flatMap(blockPart => {
      val (rowIndex, mappedColIndex, vector) = blockPart
      for {
        (colIndex, value) <- vector.iterator
      } yield (rowIndex % blockMapper.rowsPerBlock, colIndex, value)

    })

    val matrix: FlinkMatrix = SparseMatrix
      .fromCOO(blockMapper.rowsPerBlock,
        blockMapper.colsPerBlock,
        coo)
    val block = (mappedRowIndex, mappedColIndex, Block(matrix))
    block
  }

}
