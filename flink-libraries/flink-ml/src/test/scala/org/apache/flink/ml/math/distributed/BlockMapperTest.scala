package org.apache.flink.ml.math.distributed

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BlockMapperTest extends FlatSpec with Matchers with GivenWhenThen {


  val simpleMapper = new BlockMapper(90, 60, 12, 7)


  "getBlockId" should "return a valid blockId" in {
    Given("valid block coordinates")

    simpleMapper.getBlockIdByCoordinates(0, 0).get shouldBe 0
    simpleMapper.getBlockIdByCoordinates(37, 24).get shouldBe 30
    simpleMapper.getBlockIdByCoordinates(89, 59).get shouldBe simpleMapper.numBlocks-1

  }

  it should "return None" in {
    Given("non existing coordinates")
    simpleMapper.getBlockIdByCoordinates(10000, 10000) shouldBe None
    simpleMapper.getBlockIdByCoordinates(10000, 0) shouldBe None
    simpleMapper.getBlockIdByCoordinates(0, 10000) shouldBe None

  }

  it should "throw an exception" in {
    Given("invalid coordinates")
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(-1, 0)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(0, -1)
    }
  }

  "getBlockCoordinates" should "return valid coordinates" in {
    Given("a valid block id")

    simpleMapper.getBlockMappedCoordinates(3).get shouldBe(0, 3)
    simpleMapper.getBlockMappedCoordinates(0).get shouldBe(0, 0)
    simpleMapper.getBlockMappedCoordinates(9).get shouldBe(1, 0)
    simpleMapper.getBlockMappedCoordinates(10).get shouldBe(1, 1)
    simpleMapper.getBlockMappedCoordinates(15).get shouldBe(1, 6)
    simpleMapper.getBlockMappedCoordinates(48).get shouldBe(5, 3)
    simpleMapper.getBlockMappedCoordinates(70).get shouldBe(7, 7)
  }
  it should "throw an exception" in {
    Given("a non valid blockId")
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockMappedCoordinates(-1)
    }
  }
  it should "return None" in {
    Given("a non-existing blockId")
    simpleMapper.getBlockMappedCoordinates(1000000) shouldBe None
  }


  "BlockMapper constructor" should "create a valid BlockMapper" in{

    Given("valid parameters")
    val mapper=new BlockMapper(200, 60, 64, 7)
    mapper.numBlockRows shouldBe 4
    mapper.numBlockCols shouldBe 9



  }

}
