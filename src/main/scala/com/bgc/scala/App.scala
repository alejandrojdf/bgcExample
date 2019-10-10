package com.bgc.scala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {
    val LOG = Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkResources = SparkResources

    val prop = sparkResources.getPropertiesFile("C:\\Users\\aleja\\Downloads\\bgcExample\\src\\main\\resources\\")
    
    //In case that properties file need to be retrieved from HDFS
    //val prop = sparkResources.HDFSServices.getPropertiesFile("hdfs://...")
    
    //Get title.ratings.tsv
    val titleRatingsDF = sparkResources.readFileAsDF(prop.getProperty("path_ratings"))

    val top20MoviesDF = titleRatingsDF.filter(titleRatingsDF.col("numVotes") >= 50)
      .orderBy(desc("numVotes")).limit(20)

    val topMovieList = sparkResources.getDFColumnAsList(top20MoviesDF,"tconst")

    //Get title.principals.tsv
    val titlePrincipalsDF = sparkResources.readFileAsDF(prop.getProperty("path_title_principals"))
      .select(col("tconst"), col("nconst"))
      .filter(col("tconst").isin(topMovieList:_*))

    val titlePrincipalList = sparkResources.getDFColumnAsList(titlePrincipalsDF,"nconst")

    //Get name.basics.tsv
    val nameBasicsDF = sparkResources
      .readFileAsDF(prop.getProperty("path_name_basics"))
      .select(col("nconst"), col("primaryName"))
      .filter(col("nconst").isin(titlePrincipalList:_*))

    //Get title.basics.tsv
    val titleBasicsDF = sparkResources
      .readFileAsDF(prop.getProperty("path_title_basics"))
      .select(col("tconst"), col("primaryTitle"), col("originalTitle") )
      .filter(col("tconst").isin(topMovieList:_*))

    val sourceDFCount = titleRatingsDF.count()
    val averageRating = titleRatingsDF.agg(sum("averageRating")).first.getDouble(0) / sourceDFCount
    val averageNumVotes = titleRatingsDF.agg(sum("numVotes")).first.getLong(0) / sourceDFCount

    val top20RankedDF = top20MoviesDF
      .withColumn("rank",(top20MoviesDF.col("numVotes")/averageNumVotes) * averageRating)
      .orderBy(desc("rank"))
      .drop("averageRating")
      .drop("numVotes")

    val groupingCols = Seq("tconst","rank")

    val joinedDF = top20RankedDF
      .join(titlePrincipalsDF, Seq("tconst"),"inner")
      .join(nameBasicsDF,(Seq("nconst")),"inner")
      .drop("nconst")
      .groupBy(groupingCols.head, groupingCols.tail: _*)
      .agg(collect_list(col("primaryName")) as "primaryNames")
      .join(titleBasicsDF,(Seq("tconst")),"inner")
      .withColumn("titleslist", struct(col("primaryTitle"), col("originalTitle")))
      .drop("primaryTitle")
      .drop("originalTitle")
      .orderBy(desc("rank"))
      .drop("rank")

    joinedDF.show()

}
}
