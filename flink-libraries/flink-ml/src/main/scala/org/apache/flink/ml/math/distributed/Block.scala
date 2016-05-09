package org.apache.flink.ml.math.distributed

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{Matrix => FlinkMatrix}


class Block(blockData: FlinkMatrix, blockID:Option[Int]=None) {

  def toBreeze = blockData.asBreeze

  def getCols = blockData.numCols

  def getRows = blockData.numRows

  //TODO: evaluate efficiency of conversion to and from Breeze
  def multiply(other: Block) = {

    require(this.getCols == other.getRows)

    Block((blockData.asBreeze * other.toBreeze).fromBreeze)
  }


  def sum(other: Block) = Block((blockData.asBreeze + other.toBreeze).fromBreeze)


}

object Block {

  def apply(data: FlinkMatrix) = new Block(data)


}
