package com.isbanuk.di.ingestion.services
import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Class that contains all Hdfs services needed for the ingestion process.
 * @param hadoopConfiguration Hadoop configuration necessary for checking if a directory exists in HDFS
 *
 */

class HDFSServices(hadoopConfiguration: org.apache.hadoop.conf.Configuration) {
  final val hadoopConf: Configuration = hadoopConfiguration
  final val fs = FileSystem.get(hadoopConf)

  /**
   * Function that retrieves the properties from a file in HDFS
   * @param hdfsPath Hdfs path where the properties file is located
   *
   */
  final def getPropertiesFile(hdfsPath: String): Properties = {
    val path = new Path(hdfsPath)
    val hdfsFile = new BufferedReader(new InputStreamReader(fs.open(path)))
    val props: Properties = new Properties()
    props.load(hdfsFile)
    props
  }


}
