package org.scalaproject001.application


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.scalaproject001.config.AppConfig


//Refer to src/main/resources/application.conf to hard code the file paths and the integer for the top X items.

object TopXDetectedItems {
  def main(args: Array[String]): Unit = {
    //initialize logger to log messages
    val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)

    //create spark session
    val spark = SparkSession.builder()
      .appName("Top X Detected Items Per Location")
      .master("local[*]")
      .getOrCreate()

    //load config values such as the file paths
    val settings = AppConfig.settings
    val inputDetectionsPath = settings.inputDetections
    val inputLocationsPath = settings.inputLocations
    val outputPath = settings.output
    val topX = settings.topX

    //read data
    val detectionsDf = spark.read.parquet(inputDetectionsPath)
    val locationsDf = spark.read.parquet(inputLocationsPath)

    //process the data
    val dedupedRdd = deduplicateDetections(detectionsDf)
    val resultDf = computeTopItemsPerLocation(dedupedRdd, locationsDf, topX, spark, logger)

    resultDf.write.mode("overwrite").parquet(outputPath)
    logger.info(s"Output written to: $outputPath")

    spark.stop()
  }

  //the purpose of this function is to remove the duplicates
  def deduplicateDetections(detectionsDf: DataFrame): RDD[(Long, String)] = {
    detectionsDf.rdd
      .map(row => (
        row.getAs[Long]("detection_oid"),
        (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name"))
      ))
      .reduceByKey((a, _) => a)
      .map { case (_, (geoId, item)) => (geoId, item) }
  }

  //the purpose of this function is to count the top items per location
  def computeTopItemsPerLocation(
                                  dedupedRdd: RDD[(Long, String)],
                                  locationsDf: DataFrame,
                                  topX: Int,
                                  spark: SparkSession,
                                  logger: Logger
                                ): DataFrame = {

    import spark.implicits._

    //Converts locationsDf (a DataFrame) into a Scala Map so you can look up location names by their OID.
    val locationMap = locationsDf
      .rdd
      .map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))
      .collect()
      .toMap

    //Makes the locationMap available on all Spark executors without shipping it repeatedly.
    val broadcastLocMap = spark.sparkContext.broadcast(locationMap)

    val rankedRddWithWarnings = dedupedRdd
      .map { case (geoId, itemName) => ((geoId, itemName), 1) }
      .reduceByKey(_ + _)
      .map { case ((geoId, itemName), count) => (geoId, (itemName, count)) }
      .groupByKey()
      .mapValues { itemsIter =>
        val sortedItems = itemsIter.toSeq.sortBy { case (_, count) => -count }.take(topX)
        val warningsNeeded = sortedItems.size < topX
        (sortedItems, warningsNeeded)
      }

    // Collect warning messages on driver and log them using logger
    val warnings = rankedRddWithWarnings
      .filter { case (_, (_, warn)) => warn }
      .map { case (geoId, _) =>
        s"Only fewer than $topX items found for $geoId (${locationMap.getOrElse(geoId, "Unknown")})"
      }
      .collect()

    warnings.foreach(msg => logger.warn(msg))

    //Prepare final ranked RDD
    val finalRdd = rankedRddWithWarnings.flatMap { case (geoId, (items, _)) =>
      val locName = broadcastLocMap.value.getOrElse(geoId, s"Unknown($geoId)")
      items.zipWithIndex.map { case ((item, _), rank) =>
        (locName, rank + 1, item)
      }
    }

    finalRdd.toDF("geographical_location", "item_rank", "item_name")
  }

}
