package com.curioustheory.batch

import scala.reflect.runtime.universe

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.Options
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.curioustheory.util.Utilities
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window

/**
  * This is a very simple template for spark data processing pipeline to demo a solution
  *
  */
object DataPreparationTemplate {

  // LOCAL TESTING MODE - SET DEBUG TO FALSE FOR PRODUCTION ENVIRONMENT
  // --------------------------------------------------------------------------------------------------
  private val debug: Boolean = true
  private var appName: String = "DataPreparationTemplate"
  private var master: String = "local[*]"
  private var writeToFile: Boolean = false
  private var inputPath: String = "./src/test/resources/test.csv"
  private var outputPath: String = "./src/test/resources/output/"
  // --------------------------------------------------------------------------------------------------

  def main(args: Array[String]) {
    if (!debug) {

      // PROGRAM ARGUMENT
      // --------------------------------------------------------------------------------------------------
      val options: Options = new Options()
      options
        .addOption(new org.apache.commons.cli.Option("appName", true, "Application mame"))
        .addOption(new org.apache.commons.cli.Option("master", true, "Master"))
        .addOption(new org.apache.commons.cli.Option("writeToFile", true, "Write output to file"))
        .addOption(new org.apache.commons.cli.Option("inputPath", true, "File input path"))
        .addOption(new org.apache.commons.cli.Option("outputPath", false, "File output path"))

      val parser: GnuParser = new GnuParser()
      var commandLine: CommandLine = null;
      try {
        commandLine = parser.parse(options, args)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }

      appName = commandLine.getOptionValue("appName")
      master = commandLine.getOptionValue("master")
      writeToFile = commandLine.getOptionValue("writeToFile").toBoolean
      inputPath = commandLine.getOptionValue("inputPath")
      if (writeToFile) {
        outputPath = commandLine.getOptionValue("outputPath")
      }
    }

    println("============================ ARGUMENT ============================")
    println("debug: [" + debug + "]")
    println("appName: [" + appName + "]")
    println("master: [" + master + "]")
    println("writeToFile: [" + writeToFile + "]")
    println("inputPath: [" + inputPath + "]")
    if (writeToFile) {
      println("outputPath: [" + outputPath + "]")
    }
    println("==================================================================")

    // SPARK CONFIGURATIONS
    // --------------------------------------------------------------------------------------------------
    println("Initializing Spark Context...")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // This setup logging to remove lot of junk from spark, comment it out to see the difference
    Utilities.setupLogging()

    // EXECUTION OF THE JOB
    // --------------------------------------------------------------------------------------------------
    val timeStart = System.nanoTime()
    val resultDataframe: Dataset[_] = run(spark, inputPath)
    println(resultDataframe.show())

    if (writeToFile) {
      // write the result to a parquet format
      resultDataframe.write.mode(SaveMode.Overwrite).csv(outputPath)
    }

    val timeElapsed = ((System.nanoTime() - timeStart) / 1e9)
    println(appName + " completed in: " + timeElapsed + " seconds")
  }

  /**
    * Execute the data processing pipeline: loading, cleaning, feature engineering
    */
  def run(spark: SparkSession, inputPath: String): Dataset[_] = {
    // define a new header for the dataframe
    val columnNames: Seq[String] = Seq("date", "time", "open", "high", "low", "close", "volume")

    // loading the data
    val dataframe: Dataset[_] = Utilities.loadDataFormatCsv(spark, inputPath, columnNames, ",")

    // cleaning the data
    val cleanDataframe: Dataset[_] = cleanData(spark, dataframe)

    // feature engineering
    val featureDataframe: Dataset[_] = engineerFeatures(spark, cleanDataframe)

    // will be disabled for production mode, because some of these operation takes long to run
    if (debug) {
      // display a snippet of the raw data
      println("Raw Dataframe")
      println("==================================================================")
      println(dataframe.show())
      // display a summary statistics about the data
      println("Raw Dataframe Summary")
      println(dataframe.describe(dataframe.columns: _*).show())

      // display a snippet of the clean data
      println("Clean Dataframe")
      println("==================================================================")
      println(cleanDataframe.show())
      // display a summary statistics about the data
      println("Clean Dataframe Summary")
      println(cleanDataframe.describe(cleanDataframe.columns: _*).show())

      // display a snippet of the feature engineering
      println(featureDataframe.show())
      println("Feature Dataframe")
      println("==================================================================")
      // display a summary statistics about the data
      println("Feature Dataframe Summary")
      println(featureDataframe.describe(featureDataframe.columns: _*).show())
    }

    // returns the dataframe for the next job
    featureDataframe
  }

  /**
    * Clean the data for any inconsistencies, missing values, etc.
    */
  private def cleanData(spark: SparkSession, dataframe: Dataset[_]): Dataset[_] = {
    // just an example of things you can do to clean up the data
    dataframe
      .dropDuplicates("date", "time")
      .drop("volume")
      .na.fill(0, Seq("open", "high", "low", "close"))
      .filter(col("open") >= 0.75662)
      .select("date", "time", "open", "close")
      .orderBy("date", "time")
  }

  /**
    * Engineer features for machine learning or business reporting etc.
    */
  private def engineerFeatures(spark: SparkSession, dataframe: Dataset[_]): Dataset[_] = {
    // defining a window function order by unixTime in the past 10 transactions
    val window: WindowSpec = Window.orderBy("unixTime").rowsBetween(-10, 0)

    // user defined function to get day of week
    val getDayOfWeek = udf { (text: String) =>
      val date: DateTime = DateTime.parse(text, DateTimeFormat.forPattern("yyyy.MM.dd"))
      date.getDayOfWeek()
    }

    // just an example of things you can do to enhance the data
    dataframe
      .withColumn("unixTime", unix_timestamp(concat(col("date"), col("time")), "yyyy.MM.ddHH:mm"))
      .withColumn("dayOfWeek", getDayOfWeek(col("date")))
      .withColumn("openCloseDifference", round(col("close") - col("open"), 5))
      .withColumn("closeRollingAverage", round(mean(col("close")).over(window), 5))
  }
}