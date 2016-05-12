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
import org.apache.flink.ml.math.SparseVector
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

    val joinedBlocks = data.join(other.getDataset).where(x => x._1).equalTo(x => x._1)
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


  def toRowMatrix: DistributedRowMatrix = {
    val indexedRows = data
    //map id to mapped coordinates
    .map(
      block=>(blockMapper.getBlockMappedCoordinates(block._1),block)
    )
      //group by block row
    .groupBy(blockWithCoord=>blockWithCoord._1._1)
      //turn a group of blocks in a seq of rows
    .reduceGroup(blockGroup=>{

    require(blockGroup.nonEmpty)

    val groupRow=blockGroup.next()._1._1
    //all blocks must have the same row
    require(blockGroup.forall(block=>block._1._1==groupRow))

    //map every block to its mapped column
    val groupElements=blockGroup
    .map(block=>(block._1._2,block._2._2) )
    //sort by column
    .toList.sortBy(_._1)
    //unpack values
    .flatMap(
    block=>
    block._2.getBlockData.toList
    //map coordinates from block space to original space
    .map(element=>{
      val (i,j,value)=element

          (i+(groupRow*getRowsPerBlock),j+block._1,value)
    })
    )
    groupElements
    .groupBy(_._1)
    .map(row=>{
    val cooVector=row._2.map(x=>(x._2,x._3))
    IndexedRow(row._1,SparseVector.fromCOO(getNumCols,cooVector))}

    )
    }).flatMap(x => x)
    new DistributedRowMatrix(indexedRows, Some(getNumRows), Some(getNumCols))
  }
}

class MultiplyMapper(blockMapper: BlockMapper)
  extends RichMapFunction[((BlockMatrix.BlockID, Block), (BlockMatrix.BlockID, Block)), (Int, Int, Block)] {

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

