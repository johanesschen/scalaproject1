package org.scalaproject001.application

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.nio.file.Files
import org.apache.log4j.Logger
import org.scalaproject001.config.AppSettings
import java.io.File

class TopXIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  // sets up a lazy spark session
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("TopX Integration Test")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  //the purpose of this test is to run the entire logic end to end flow
  test("Integration: Main flow from Parquet input to output") {
    import spark.implicits._

    // Prepare temporary directories
    val tempDir = Files.createTempDirectory("topx-test").toFile
    val inputDetectionsPath = new File(tempDir, "detections").getAbsolutePath
    val inputLocationsPath = new File(tempDir, "locations").getAbsolutePath
    val outputPath = new File(tempDir, "output").getAbsolutePath

    // Create mock input data
    val detectionsDf = Seq(
      (1L, 101L, "Bottle"),
      (2L, 101L, "Bottle"),
      (3L, 101L, "Bag"),
      (4L, 102L, "Phone"),
      (5L, 102L, "Phone"),
      (6L, 102L, "Tablet")
    ).toDF("detection_oid", "geographical_location_oid", "item_name")

    val locationsDf = Seq(
      (101L, "Park"),
      (102L, "Mall")
    ).toDF("geographical_location_oid", "geographical_location")

    detectionsDf.write.mode("overwrite").parquet(inputDetectionsPath)
    locationsDf.write.mode("overwrite").parquet(inputLocationsPath)

    // Use a custom AppSettings instance instead of reading from .conf
    val settings = AppSettings(
      inputDetections = inputDetectionsPath,
      inputLocations = inputLocationsPath,
      output = outputPath,
      topX = 2
    )

    // Run the actual logic (bypass main() for testability)
    val dedupedRdd = TopXDetectedItems.deduplicateDetections(
      spark.read.parquet(settings.inputDetections)
    )

    val resultDf = TopXDetectedItems.computeTopItemsPerLocation(
      dedupedRdd,
      spark.read.parquet(settings.inputLocations),
      settings.topX,
      spark,
      Logger.getLogger("IntegrationTest")
    )

    resultDf.write.mode("overwrite").parquet(settings.output)

    // Validate the output
    val outputDf = spark.read.parquet(settings.output)
    val outputSet = outputDf.collect().map(r => (r.getString(0), r.getInt(1), r.getString(2))).toSet

    val expected = Set(
      ("Park", 1, "Bottle"),
      ("Park", 2, "Bag"),
      ("Mall", 1, "Phone"),
      ("Mall", 2, "Tablet")
    )

    assert(outputSet == expected)

    // Clean up if desired
    tempDir.deleteOnExit()
  }
}
