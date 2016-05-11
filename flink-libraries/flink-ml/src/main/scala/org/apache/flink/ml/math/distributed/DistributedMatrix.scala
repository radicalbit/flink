package org.apache.flink.ml.math.distributed

trait DistributedMatrix {

  def getNumRows:Int
  def getNumCols:Int
}
