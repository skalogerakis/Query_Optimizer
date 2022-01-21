package mainTestPackage

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object MainTester {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .config("spark.sql.streaming.checkpointLocation","/home/skalogerakis/TUC_Projects/SparkTest/Checkpoint")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("Error")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}



