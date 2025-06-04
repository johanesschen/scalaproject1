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

## Setup
Download SBT and ensure that it is added to path so that we are able to call it from within intelliJ.


Other Notes: 

Task 1 Point 3: Please note that the file paths are coded in the application.conf file 

Task 1 Scoring criteria b regarding integration and unit tests: Refer to src/test/scala and check TopXDetectedItemsTest and TopXIntegrationTest

Task 1 Scoring criteria g: Refer to the file 'Design Considerations'

Task 2 Point 1: Refer to Salting_CodeSnippet for the code snippet requested. 

---------------------

Key Functions:

- deduplicateDetections(detectionsDf: DataFrame) -> RDD[(Long, String)]
  - Removes duplicate detections to ensure each item-location pair appears only once.
  - Uses reduceByKey to retain one occurrence per detection.

- computeTopItemsPerLocation(...) -> DataFrame
  - Counts occurrences of detected items per location.
  - Sorts items by frequency and retains the top X items.
  - Broadcasts location mappings for efficiency.

- Logs warnings if a location has fewer than X items detected.

-----------------------

Annex: For first timers coding in Scala and Spark 

What is Scala?
Scala (short for Scalable Language) is a modern programming language that:
Runs on the Java Virtual Machine (JVM)
Combines object-oriented and functional programming
Can use all existing Java libraries
Is concise, expressive, and powerful

What is Apache Spark?
Apache Spark is an open-source big data processing engine used for:
Distributed data processing
Real-time analytics
Machine learning pipelines
Graph processing
ETL jobs (Extract, Transform, Load)
Spark works on clusters and handles massive amounts of data in memory

What is an RDD?
RDD stands for Resilient Distributed Dataset. It's the core abstraction in Apache Spark for working with distributed data.
Think of an RDD as a fault-tolerant, distributed collection of elements that you can process in parallel across a cluster.

What is a Parquet File?
A Parquet file is a columnar storage format commonly used in big data processing frameworks like Apache Spark, Hive, Presto, and others.
Apache Parquet is a file format designed for efficient data storage and retrieval. It stores data column-by-column, rather than row-by-row like CSV or JSON.

Why is Scala commonly used with Apache Spark?

✅ 1.1. Spark is written in Scala
Apache Spark is natively implemented in Scala.
All core Spark APIs were first designed in Scala.
This means:
Scala gets the latest features first.
Spark's internal optimizations are best exposed in Scala.

✅ 1.2. Performance
Scala runs on the JVM, which is compiled and fast.
Compared to Python (PySpark), Scala avoids Python’s:
Serialization overhead
Interpreter bottlenecks
This results in better performance, especially for:
Complex transformations
Machine learning pipelines
Streaming jobs

✅ 1.3. Advanced API Access
Scala lets you access low-level Spark APIs like:
RDD transformations
Custom partitioning
Catalyst optimizer internals
These are not easily accessible (or exposed) in Python or R.

✅ 1.4. Type Safety and Functional Features
Scala has strong static typing, which:
Catches many errors at compile time
Helps in writing large, maintainable codebases
It also supports functional programming, which aligns naturally with:
Spark’s use of map, flatMap, reduce, etc.

✅ 1.5. Better Integration with Spark Tools
Many Spark ecosystem tools are Scala-first:
Spark MLlib
GraphX
Custom UDFs
You can write custom extensions or connectors more easily in Scala.

SBT expects this structure:

project/
  build.properties
  plugins.sbt       ← optional SBT plugin configs
src/
  main/
    scala/          ← your Scala source code
  resources/      ← config files, application.conf, etc.
  test/
    scala/          ← test code
    resources/      ← test-specific configs
target/             ← SBT build output (SHOULD be gitignored)
build.sbt           ← main build configuration
.gitignore          ← include temp & auto-generated files


----------------

Before this code can be deployed to Production, please note that there is a vulnerable CVE related to the use of log4j 1.2.17 which has to be resolved before deployment. 
Either the related code can be removed, or the log4j version used can be of a higher version where the vulnerability is already addressed. 
