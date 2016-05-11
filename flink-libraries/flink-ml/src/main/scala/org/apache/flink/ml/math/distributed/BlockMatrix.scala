package org.apache.flink.ml.math.distributed


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.distributed.BlockMatrix.{BlockID, MatrixFormatException, WrongMatrixCoordinatesException}


//TODO: test EVERYTHING
class BlockMatrix(//actual data
                  data: DataSet[(BlockID, Block)],
                  blockMapper: BlockMapper
                   ) extends DistributedMatrix {

  val getDataset = data

  val getNumCols = blockMapper.numCols
  val getNumRows = blockMapper.numRows

  val getBlockCols = blockMapper.numBlockCols
  val getBlockRows = blockMapper.numBlockRows

  val getRowsPerBlock = blockMapper.rowsPerBlock
  val getColsPerBlock = blockMapper.colsPerBlock

  def multiply(other: BlockMatrix): BlockMatrix = {

    require(this.getBlockCols == other.getBlockRows)
    require(this.getColsPerBlock == other.getColsPerBlock)
    require(this.getRowsPerBlock == other.getRowsPerBlock)

    val joinedBlocks = data.join(other.getDataset).where(0).equalTo(1)
    val multipliedJoinedBlocks = joinedBlocks.map(x => {
      val ((idl, left), (idr, right)) = x
      val (i, j) = blockMapper.getBlockMappedCoordinates(idl)
      val (s, t) = blockMapper.getBlockMappedCoordinates(idr)
      require(j == s)
      (i, t, left.multiply(right))

    })

    val reducedBlocks = multipliedJoinedBlocks.reduce(
      (x, y) => {
        (x._1, x._2, x._3 sum y._3)
      }
    )
      .map(block => {

        val blockID = blockMapper.getBlockIdByCoordinates(block._1, block._2)

        (blockID, block._3)
      })
    new BlockMatrix(reducedBlocks,
      //TODO: check on paper
      new BlockMapper(other.getNumRows, this.getNumCols, this.blockMapper.rowsPerBlock, this.blockMapper.colsPerBlock))
  }

}

object BlockMatrix {

  type BlockID = Int

  class MatrixFormatException(message: String) extends Exception(message)

  class WrongMatrixCoordinatesException(message: String) extends Exception(message)

}

