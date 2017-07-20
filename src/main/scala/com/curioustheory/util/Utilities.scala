package com.curioustheory.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object Utilities {

  /**
   * Setup default logging
   */
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /**
   * Loading csv data from the given path
   *
   * More spark csv options can be found here:
   *   https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame
   */
  def loadDataFormatCsv(spark: SparkSession, filePath: String, header: Seq[String], separator: String): Dataset[_] = {
    println("Loading files from " + filePath)
    // you can also read different format: parquet, hive, sql etc. also configure the options too.
    val dataframe: Dataset[_] = spark
      .read
      .option("header", header.isEmpty)
      .option("sep", separator)
      .csv(filePath)

    if (!header.isEmpty) {
      // in the case where no column names are defined, you can add your own
      dataframe.toDF(header: _*)
    } else {
      dataframe
    }
  }

  /**
   * Loading parquet data from the given path
   */
  def loadDataFormatParquet(spark: SparkSession, filePath: String): Dataset[_] = {
    val dataframe: Dataset[_] = spark
      .read
      .parquet(filePath)
    dataframe
  }
}