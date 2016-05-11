package org.apache.flink.ml.math.distributed

case class BlockMapper(//original matrix size
                       numRows: Int, numCols: Int,
                       //block size
                       rowsPerBlock: Int, colsPerBlock: Int

                        ) {

  val numBlockRows: Int = math.ceil(numRows * 1.0 / rowsPerBlock).toInt
  val numBlockCols: Int = math.ceil(numCols * 1.0 / colsPerBlock).toInt
  val numBlocks = numBlockCols * numBlockRows

  def getBlockIdByCoordinates(i: Int, j: Int): Int = {

    if (i < 0 || j < 0||i >= numRows || j >= numCols)
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")

    else {


      val mappedRow = i / rowsPerBlock
      val mappedColumn = j / colsPerBlock
      val res = mappedRow * numBlockCols + mappedColumn

      assert(res <= numBlocks)
      res
    }
  }

  def getBlockMappedCoordinates(blockId: Int): (Int, Int) = {
    if (blockId < 0||blockId > numBlockCols * numBlockRows)
      throw new IllegalArgumentException(s"BlockId numeration starts from 0. $blockId is not a valid Id")

    else {
      val i = blockId / numBlockCols
      val j = blockId % numBlockCols
     (i, j)
    }
  }


  /**
   * Translates absolute coordinates to the mapped coordinates of the block these coordinates belong to
   * @param i
   * @param j
   * @return
   */
  def absCoordToMappedCoord(i: Int, j: Int): (Int, Int) =
    getBlockMappedCoordinates(getBlockIdByCoordinates(i, j))

  def getBlockIdByMappedCoord(i: Int, j: Int): Int = {

    if (i < 0 || j < 0 || i >= numBlockRows || j >= numBlockCols)
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")

    else
      i * numBlockCols + j

  }


}


