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

import org.apache.flink.api.common.functions.{MapFunction, RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.math.distributed.BlockMatrix.BlockID
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

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

  //TODO: broken
  def multiply(other: BlockMatrix): BlockMatrix = {

    require(this.getBlockCols == other.getBlockRows)
    require(this.getColsPerBlock == other.getColsPerBlock)
    require(this.getRowsPerBlock == other.getRowsPerBlock)

    val otherWithCoord = other.getDataset.map(new MapToMappedCoord(blockMapper))

    val joinedBlocks = data.map(
      new MapToMappedCoord(blockMapper)).
      join(otherWithCoord)

      .where(x => x._2)
      .equalTo(x => x._1)

    val multipliedJoinedBlocks = joinedBlocks.map(new MultiplyMapper(blockMapper))

    multipliedJoinedBlocks.collect().foreach { x =>
      val toPrint = x._4.toBreeze
      println(toPrint)
    }



    val reducedBlocks = multipliedJoinedBlocks.groupBy(0).reduceGroup(group => {
      group.reduce((x, y) => {
        (x._1, y._2, x._3, x._4 sum y._4)
      })
    }
    ).map(x => (x._2, x._3, x._4))
      .map(new MapToBlockID(blockMapper))
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
      new MapToMappedCoord(blockMapper)
    )
      //group by block row
    .groupBy(blockWithCoord=>blockWithCoord._1)
      //turn a group of blocks in a seq of rows
    .reduceGroup( new ToRowMatrixReducer(blockMapper))
    new DistributedRowMatrix(indexedRows, Some(getNumRows), Some(getNumCols))
  }
}

object BlockMatrix {

  type BlockID = Int

  class MatrixFormatException(message: String) extends Exception(message)

  class WrongMatrixCoordinatesException(message: String) extends Exception(message)

}

class MapToBlockID(blockMapper: BlockMapper) extends MapFunction[(Int, Int, Block), (Int, Block)] {
  override def map(block: (BlockID, BlockID, Block)): (BlockID, Block) = {

    val blockID = blockMapper.getBlockIdByCoordinates(block._1, block._2)

    (block._1, block._3)
  }
}

class MapToMappedCoord(blockMapper: BlockMapper) extends MapFunction[(Int, Block), (Int, Int, Block)] {
  override def map(value: (BlockID, Block)): (Int, Int, Block) = {
    val (i, j) = blockMapper.getBlockMappedCoordinates(value._1)
    (i, j, value._2)
  }
}


class MultiplyMapper(blockMapper: BlockMapper)
  extends RichMapFunction[((Int, Int, Block), (Int, Int, Block)), (Int, Int, Int, Block)] {

  override def map(value: ((Int, Int, Block), (Int, Int, Block))): (Int, Int, Int, Block) = {
    val ((i, j, left), (s, t, right)) = value

    require(j == s, s"Block rows and column must match $j - $s")
    (j, i, t, left.multiply(right))
  }
}

class ToRowMatrixReducer(blockMapper: BlockMapper) extends RichGroupReduceFunction[
  (Int, Int, Block), IndexedRow] {
  override def reduce(
                       values: lang.Iterable[(Int, Int, Block)],
                       out: Collector[IndexedRow]): Unit = {
    val blockGroup = values.toList
    require(blockGroup.nonEmpty)

    val groupRow = blockGroup.head._1
    //all blocks must have the same row
    require(blockGroup.forall(block => block._1 == groupRow))

    //map every block to its mapped column
    val groupElements = blockGroup.map(block=>(block._2,block._3) )
  //sort by column
  .sortBy(_._1)
  //unpack values
  .flatMap(
  block=>
  block._2.getBlockData.toList
//map coordinates from block space to original space
.map(element=>{
val (i,j,value)=element

(i+(groupRow*blockMapper.rowsPerBlock),j+block._1*blockMapper.colsPerBlock,value)
})
)
    groupElements
      .groupBy(_._1)
      .foreach(row => {
        val cooVector = row._2.map(x => (x._2, x._3))
        out.collect(IndexedRow(row._1, SparseVector.fromCOO(blockMapper.numCols, cooVector)))
      }

      )

  }

}
