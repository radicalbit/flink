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


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.distributed.BlockMatrix.BlockID


//TODO: test EVERYTHING
class BlockMatrix(
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

  val getNumBlocks = blockMapper.numBlocks

  def multiply(other: BlockMatrix): BlockMatrix = {

    require(this.getBlockCols == other.getBlockRows)
    require(this.getColsPerBlock == other.getColsPerBlock)
    require(this.getRowsPerBlock == other.getRowsPerBlock)

    val joinedBlocks = data.join(other.getDataset).where(x=>x._1).equalTo(x=>x._1)
    val multipliedJoinedBlocks = joinedBlocks.map(new MultiplyMapper(blockMapper))

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

      BlockMapper(other.getNumRows,
        this.getNumCols,
        this.blockMapper.rowsPerBlock,
        this.blockMapper.colsPerBlock
      )
    )
  }

}

class MultiplyMapper (blockMapper:BlockMapper)
  extends RichMapFunction[((BlockMatrix.BlockID, Block), (BlockMatrix.BlockID, Block)),(Int, Int, Block)]{

  override def map(value: ((BlockMatrix.BlockID, Block), (BlockMatrix.BlockID, Block))): (Int, Int, Block) = {
    val ((idl, left), (idr, right)) = value
    val (i, j) = blockMapper.getBlockMappedCoordinates(idl)
    val (s, t) = blockMapper.getBlockMappedCoordinates(idr)
    require(j == s)
    (i, t, left.multiply(right))
  }
}

object BlockMatrix {

  type BlockID = Int

  class MatrixFormatException(message: String) extends Exception(message)

  class WrongMatrixCoordinatesException(message: String) extends Exception(message)

}

