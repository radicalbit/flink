package org.apache.flink.ml.math.distributed

import java.lang

import breeze.linalg.{CSCMatrix => BreezeSparseMatrix, Matrix => BreezeMatrix, Vector => BreezeVector}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.ml.math.{Matrix => FlinkMatrix, _}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

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

  def toCoo: Seq[(Int, Int, Double)] = {

    val localRows = data.collect()

    for (
      IndexedRow(rowIndex, vector) <- localRows;

      (columnIndex, value) <- vector
    ) yield (rowIndex, columnIndex, value)

  }

  def toLocalSparseMatrix: SparseMatrix = {


    val localMatrix = SparseMatrix.fromCOO(this.numRows, this.numCols, this.toCoo)
    require(localMatrix.numRows == this.numRows)
    require(localMatrix.numCols == this.numCols)
    localMatrix
  }

  //TODO: convert to dense representation on the distributed matrix and collect it afterward
  def toLocalDenseMatrix: DenseMatrix = this.toLocalSparseMatrix.toDenseMatrix


  def toBlockMatrix(rowsPerBlock: Int = 1024, colsPerBlock: Int = 1024): BlockMatrix = {

    val rowGroupReducer = new RowGroupReducer(rowsPerBlock, colsPerBlock, numRows, numCols)

    
    val blocks = data
      .groupBy(row => row.rowIndex % rowsPerBlock)
      .reduceGroup(rowGroupReducer)

    val blockMapper = new BlockMapper(numRows, numCols, rowsPerBlock, colsPerBlock)

    new BlockMatrix(blocks, blockMapper)
  }


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
  def fromCOO(data: DataSet[(Int, Int, Double)], numRows: Int, numCols: Int, isSorted: Boolean = false): DistributedRowMatrix
  = {
    val vectorData: DataSet[(Int,SparseVector)] = data
      .groupBy(0)
      .reduceGroup(sparseRow => {
        require(sparseRow.nonEmpty)
        val sortedRow =
          if (isSorted)
            sparseRow.toList
          else
            sparseRow.toList.sortBy(row => row._2)

        val (indices, values) = sortedRow.map(x => (x._2, x._3)).toList.unzip
        (sortedRow.head._1,SparseVector(numCols, indices.toArray, values.toArray))
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
}

class RowGroupReducer(rowsPerBlock: Int, colsPerBlock: Int, numRows: Int, numCols: Int) extends RichGroupReduceFunction[IndexedRow, (Int, Block)] {

  import org.apache.flink.ml.math.Breeze._

  override def reduce(values: lang.Iterable[IndexedRow], out: Collector[(Int, Block)]): Unit = {
    val blockMapper = new BlockMapper(numRows, numCols, rowsPerBlock, colsPerBlock)

    val sortedRows = values.toList.sorted
    require(sortedRows.max.rowIndex - sortedRows.min.rowIndex <= rowsPerBlock)
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


        val blockID = blockMapper.getBlockIdByCoordinates(i, j) match {
          case Some(id) => id
          case None => throw new Exception("Invalid block coordinates requested.")

        }
        out.collect((blockID, block))
      })


  }

  def calculateSlices(): List[(Int, Int)] =

    (0 to (math.ceil(numCols * 1.0 / colsPerBlock) - 1).toInt)
      .map(
        x => {

          val start = x * colsPerBlock

          val endT = (x + 1) * colsPerBlock - 1
          val end = if (endT > numCols - 1)
            numCols - 1
          else
            endT

          (start, end)
        }
      ).toList


  //Create a Block with mapped coordinates from an intermediate unstructured block
  def createBlock(data: List[(Int, Int, Vector)], blockMapper: BlockMapper): (Int, Int, Block) = {
    require(data.nonEmpty)
    val mappedColIndex = data.head._2
    val mappedRowIndex = blockMapper.getBlockIdByCoordinates(data.head._1, 0).get
    val coo = data.flatMap(blockPart => {
      val (rowIndex, mappedColIndex, vector) = blockPart
      for {
        (colIndex, value) <- vector.iterator
      } yield (rowIndex, colIndex, value)

    })

    val matrix: FlinkMatrix = SparseMatrix.fromCOO(
      blockMapper.rowsPerBlock, blockMapper.colsPerBlock, coo)
    (mappedRowIndex, mappedColIndex, Block(matrix))
  }

}
