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

    //val HDFSServices = new HDFSServices(spark.sparkContext.hadoopConfiguration)

  
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

  /**
   * Function that calculate the Spark Resources config for the ingestion
   *
   * @param fileSize File size in GB to calculate dynamically the resources needed.
   *
   */
  def getClusterResources(fileSize: Int): ClusterResources =  {
    fileSize match {
      case  0 => new ClusterResources("4","6G","6G")
      case size if (fileSize > 0) && (fileSize <= 2) => new ClusterResources("6","8G","8G")
      case size if (fileSize > 2) && (fileSize <= 3) => new ClusterResources("12","10G","10G")
      case size if (fileSize > 3) && (fileSize <= 6) => new ClusterResources("14","7G","7G")
      case size if (fileSize > 6) && (fileSize <= 7) => new ClusterResources("14","8G","8G")
      case size if (fileSize > 7) && (fileSize <= 15) => new ClusterResources("16","8G","8G")
      case size if (fileSize > 15) && (fileSize <= 20) => new ClusterResources("18","9G","9G")
      case size if (fileSize > 20) && (fileSize <= 100) => new ClusterResources("20","10G","10G")
      case size if fileSize > 100 => new ClusterResources("24","10G","10G")
      case _ => new ClusterResources("10","4G","4G")
    }
  }

  /**
   * Class that assign the Spark Resources calculated in the previous method
   * @param numExecutors Number of executors
   * @param executorMem Executors memory
   * @param driverMem Driver memory
   *
   */

  class ClusterResources(numExecutors: String, executorMem: String, driverMem: String ) {
    val executorsInstances: String = numExecutors
    val executorMemory: String = executorMem
    val driverMemory: String = driverMem
  }

}
