package org.apache.flink.ml.math.distributed

class BlockMapper(//original matrix size
                  val numRows: Int, val numCols: Int,
                  //block size
                  val rowsPerBlock: Int, val colsPerBlock: Int

                   ) {

  val numBlockRows: Int = math.ceil(numRows * 1.0 / rowsPerBlock).toInt
  val numBlockCols: Int = math.ceil(numCols * 1.0 / colsPerBlock).toInt
  val numBlocks = numBlockCols * numBlockRows

  def getBlockIdByCoordinates(i: Int, j: Int): Option[Int] = {

    if (i < 0 || j < 0)
      throw new IllegalArgumentException("Invalid coordinates.")

    if (i >= numRows || j >= numCols)
      None
    else {


      val mappedRow = i / rowsPerBlock
      val mappedColumn = j / colsPerBlock
      val res = mappedRow * numBlockCols + mappedColumn

      assert(res <= numBlocks)
      Some(res)
    }
  }

  def getBlockMappedCoordinates(blockId: Int): Option[(Int, Int)] = {
    if (blockId < 0)
      throw new IllegalArgumentException(s"BlockId numeration starts from 0. $blockId is not a valid Id")

    if (blockId > numBlockCols * numBlockRows)
      None
    else {
      val i = math.ceil(blockId / numBlockRows).toInt
      val j = math.ceil(blockId - i * numBlockCols).toInt
      Some((i, j))
    }
  }
}


