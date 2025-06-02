# Top X Detected Items per Location (Spark + Scala)

This Spark RDD-based program computes the top X most frequently detected items per geographical location, based on object detection logs.

## Features
- Fully configurable via `.conf` file or environment variables
- Optimized using broadcast join (no `.join`)
- Clean code, modularized functions
- Reusable logic via functional injection of ranking algorithm
- Output written as Parquet

## Run Instructions
1. Ensure you have an `application.conf` file in `resources/`
2. Use `sbt package` to generate the JAR
3. Submit using `spark-submit` or run via IntelliJ with Spark plugin

## Required Inputs
- Dataset A (detections) in Parquet
- Dataset B (geographical locations) in Parquet

## Output
A Parquet file containing:
- `geographical_location`
- `item_rank`
- `item_name`
