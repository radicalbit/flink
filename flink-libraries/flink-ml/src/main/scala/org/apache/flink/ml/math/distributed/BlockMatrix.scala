package org.apache.flink.ml.math.distributed



import org.apache.flink.api.scala._
import org.apache.flink.ml.common.Block
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._Block



//TODO: test EVERYTHING
class BlockMatrix(//actual data
                  data:DataSet[(Int,Int,Block)],
                  blockMapper:BlockMapper
                  ) extends DistributedMatrix{

  val getDataset=data

  val getNumCols=blockMapper.numCols
  val getNumRows=blockMapper.numRows

  val getBlockCols = blockMapper.numBlockCols
  val getBlockRows = blockMapper.numBlockRows

  val getRowsPerBlock=blockMapper.rowsPerBlock
  val getColsPerBlock=blockMapper.colsPerBlock

  def multiply(other:BlockMatrix):BlockMatrix={

    require(this.getBlockCols==other.getBlockRows)
    require(this.getColsPerBlock==other.getColsPerBlock)
    require(this.getRowsPerBlock==other.getRowsPerBlock)

    val joinedBlocks=data.join(other.getDataset).where(0).equalTo(1)
    val multipliedJoinedBlocks=joinedBlocks.map(x=>{
      val ((i,j,left),(s,t,right)) =  x
      require(j==s)
      (i,t,left.multiply(right))

    })

    val reducedBlocks=multipliedJoinedBlocks.reduce(
      (x,y)=>
        {
          (x._1,x._2,x._3 sum y._3)
        }
    )
    new BlockMatrix(reducedBlocks,
      new BlockMapper(other.getNumRows,this.getNumCols,this.blockMapper.rowsPerBlock,this.blockMapper.colsPerBlock))
  }


}
