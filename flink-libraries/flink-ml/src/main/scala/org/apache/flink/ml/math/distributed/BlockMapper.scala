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

case class BlockMapper(//original matrix size
                  val numRows: Int, val numCols: Int,
                  //block size
                  val rowsPerBlock: Int, val colsPerBlock: Int
                   ) {

  val numBlockRows: Int = math.ceil(numRows * 1.0 / rowsPerBlock).toInt
  val numBlockCols: Int = math.ceil(numCols * 1.0 / colsPerBlock).toInt
  val numBlocks = numBlockCols * numBlockRows

  def getBlockIdByCoordinates(i: Int, j: Int): Int = {

    if (i < 0 || j < 0 || i >= numRows || j >= numCols) {
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")
    }

    else {
      val mappedRow = i / rowsPerBlock
      val mappedColumn = j / colsPerBlock
      val res = mappedRow * numBlockCols + mappedColumn

      assert(res <= numBlocks)
      res
    }
  }

  def getBlockMappedCoordinates(blockId: Int): (Int, Int) = {
    if (blockId < 0 || blockId > numBlockCols * numBlockRows) {
      throw new IllegalArgumentException(
        s"BlockId numeration starts from 0. $blockId is not a valid Id"
      )
    }

    else {
      val i = blockId / numBlockCols
      val j = blockId % numBlockCols
      (i, j)
    }
  }


  /**
   * Translates absolute coordinates to the mapped coordinates of the block
   * these coordinates belong to.
   * @param i
   * @param j
   * @return
   */
  def absCoordToMappedCoord(i: Int, j: Int): (Int, Int) =
    getBlockMappedCoordinates(getBlockIdByCoordinates(i, j))

  def getBlockIdByMappedCoord(i: Int, j: Int): Int = {

    if (i < 0 || j < 0 || i >= numBlockRows || j >= numBlockCols) {
      throw new IllegalArgumentException(s"Invalid coordinates ($i,$j).")
    }

    else {
      i * numBlockCols + j
    }

  }


}


