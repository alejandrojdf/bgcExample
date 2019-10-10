**Project have the following structure**
- App: backbone of the project, where all the dataframe transformation are being made
- SparkResources: contains all the basic configuration for Spark session and some methods related to DF processing, also contains some unused method (due to lack of time and environment limitation) that can dynamically calculate the spark resources needed. 
- HDFSServices: contains a small method to get the properties from HDFS (in case it's required).

**ETL Steps**:
- First at all, sources files are read.
- Then we get the top 20 movies with a minimum of 50 votes with the ranking determined by:

    `        (numVotes/averageNumberOfVotes) * averageRating   ` 
- After that we perform a series of joiner, aggregation and other transformations to the data in order to get the list of the persons who are most often credited and list the different titles of the 20 movies.

**Resources**:

The file contains a project.properties file in the resources folder, which contains the path to all the needed sources, so it is not need to be hardcoded.

In case that the source files path change more dynamically, the path can be placed outside the project, so there is no need to package and re-deploy the software.

The file also can be configured to be taken from HDFS, to avoid any path problems when running in yarn cluster mode.


**Running the project**:
- First, please git clone the project and wait for the Maven dependencies to be indexed and downloaded (all dependencies are public).
- After this, please package the jar file using Maven.
- Ir order to run the jar file:
    - Local mode:
    
    `spark-submit --master local[*] --deploy-mode client --class com.bgc.scala.App C:\Users\aleja\Downloads\bgcExample\target\bgcExample-1.0-SNAPSHOT.jar `
    - Cluster mode (Spark standalone mode):
    
    `spark-submit --master spark://IP:PORT --num-executors=2 --class com.bgc.scala.App C:\Users\aleja\Downloads\bgcExample\target\bgcExample-1.0-SNAPSHOT.jar `

**Testing**:
Two tests were performed, local mode and cluster mode. For the second test, a standalone local cluster was created with one master and 3 executors, both tests were successful and provided the following result:

|   tconst|        primaryNames|          titleslist|
--- | --- | ---
|tt0111161|[Stephen King, Bo...|[The Shawshank Re...|
|tt0468569|[Lorne Orleans, M...|[The Dark Knight,...|
|tt1375666|[Hans Zimmer, Chr...|[Inception, Incep...|
|tt0137523|[Ross Grayson Bel...|[Fight Club, Figh...|
|tt0110912|[Bruce Willis, La...|[Pulp Fiction, Pu...|
|tt0109830|[Gary Sinise, Rob...|[Forrest Gump, Fo...|
|tt0944947|[Kit Harington, E...|[Game of Thrones,...|
|tt0133093|[Carrie-Anne Moss...|[The Matrix, The ...|
|tt0120737|[J.R.R. Tolkien, ...|[The Lord of the ...|
|tt0167260|[Fran Walsh, Pete...|[The Lord of the ...|
|tt0068646|[Mario Puzo, Fran...|[The Godfather, T...|
|tt1345836|[Gary Oldman, Ann...|[The Dark Knight ...|
|tt0167261|[J.R.R. Tolkien, ...|[The Lord of the ...|
|tt0816692|[Emma Thomas, Jes...|[Interstellar, In...|
|tt0114369|[Richard Francis-...|      [Se7en, Se7en]|
|tt0903747|[Steven Michael Q...|[Breaking Bad, Br...|
|tt1853728|[Kerry Washington...|[Django Unchained...|
|tt0172495|[Russell Crowe, R...|[Gladiator, Gladi...|
|tt0372784|[Christopher Nola...|[Batman Begins, B...|
|tt0848228|[Robert Downey Jr...|[The Avengers, Th...|

Please find the following screenshots with the test performed in Cluster mode.

**Final notes**:
- jdk1.8.0_191 used
- Although on my daily basis I always focus on the details and try to be as much strict as possible in good practices, due to the lack of time it was unfeasible to perform unit and integration tests, and some inconsistencies could result in naming conventions, etc. All effort was focused on producing the expected results.
- No `spark.sql(...)` statements were used as required.
- All the DataFrames were filtered before joining or aggregating, in order to improve the performance and reduce shuffle.
- The result of the process is a standard output as no output was specified, such as HDFS csv file, a Hive or a NOSQL table.