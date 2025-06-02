package org.scalaproject001.application

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class TopXDetectedItemsTest extends AnyFunSuite with BeforeAndAfterAll {

  // ✅ Use lazy val to ensure spark is a stable identifier
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("TopX Test")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("deduplicateDetections should remove duplicates and return correct RDD") {
    import spark.implicits._

    val inputDf = Seq(
      (1L, 101L, "Bottle"),
      (1L, 101L, "Bottle"), // duplicate detection_oid
      (2L, 102L, "Bag")
    ).toDF("detection_oid", "geographical_location_oid", "item_name")

    val result: RDD[(Long, String)] = TopXDetectedItems.deduplicateDetections(inputDf)

    val output = result.collect().toSet
    val expected = Set((101L, "Bottle"), (102L, "Bag"))

    assert(output == expected)
  }

  test("computeTopItemsPerLocation should rank items and resolve location names") {
    import spark.implicits._

    val dedupedRdd: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (101L, "Bottle"),
      (101L, "Bottle"),
      (101L, "Bag"),
      (102L, "Phone"),
      (102L, "Phone"),
      (102L, "Tablet")
    ))

    val locationDf = Seq(
      (101L, "Park"),
      (102L, "Mall")
    ).toDF("geographical_location_oid", "geographical_location")

    val logger = org.apache.log4j.Logger.getLogger("TestLogger")

    val outputDf: DataFrame = TopXDetectedItems.computeTopItemsPerLocation(
      dedupedRdd,
      locationDf,
      2,
      spark,
      logger
    )

    val result = outputDf.collect().map(row => (row.getString(0), row.getInt(1), row.getString(2))).toSet
    val expected = Set(
      ("Park", 1, "Bottle"),
      ("Park", 2, "Bag"),
      ("Mall", 1, "Phone"),
      ("Mall", 2, "Tablet")
    )

    assert(result == expected)
  }
}
