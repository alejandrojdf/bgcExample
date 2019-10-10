package com.bgc.scala

import java.util.Properties

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object SparkResources extends Serializable {
  val spark = SparkSession.builder
    .appName("bgcExample")
    .enableHiveSupport()
    .getOrCreate

  /**Function reads from a file to a DataFrame
   *
   * @param path Path to the source file
   * @return Dataframe
   */
  def readFileAsDF(path: String): DataFrame ={
    spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(s"$path")
  }

  /** Get properties file
   *
   * @param path properties file path
   * @return properties object
   */
  final def getPropertiesFile(path: String): Properties = {
    var properties : Properties = null

    val path = getClass.getResource("/project.properties")
    val source = Source.fromURL(path)

      properties = new Properties()
      properties.load(source.bufferedReader())
    properties
  }

  /**Get list from a DataFrame column values
   *
   * @param DF Source DF
   * @param columnName Column name to be retrieved
   * @return Sequence with the values
   */
  def getDFColumnAsList(DF: DataFrame, columnName: String): Seq[String] ={
    DF.select(col(columnName))
      .collect()
      .map(row => row.getString(0).replaceAll("[\\[\\]]","")).toSeq
  }

}
