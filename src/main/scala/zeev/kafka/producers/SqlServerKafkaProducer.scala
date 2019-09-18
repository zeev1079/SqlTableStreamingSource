package zeev.kafka.producers
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import zeev.spark.streaming.sources.SqlServerStreamingSource

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

class SqlServerKafkaProducer extends App {
  val spark =
  {
    val master = "yarn"
  val appName = "SqlServerDataToKafka"
    val conf:SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.dynamicAllocation.enabled","false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.yarn.maxappattempts","1")
    SparkSession.builder().config(conf).getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")
  val SQL_SOURCE_CLASS = SqlServerStreamingSource.getClass.getCanonicalName
  val SQL_SOURCE_CLASS_NAME = SQL_SOURCE_CLASS.substring(0, SQL_SOURCE_CLASS.indexOf("$"))

  spark.readStream
    .format(SQL_SOURCE_CLASS_NAME)
    .option(SqlServerStreamingSource.WINDOW_JUMP,"1 minutes")
    .option(SqlServerStreamingSource.WINDOW_SIZE,"1 minutes")
    .option(SqlServerStreamingSource.DELAY,"1 minutes")
    .option(SqlServerStreamingSource.NUM_PARTITIONS,spark.sparkContext.defaultParallelism)//get be whater ever u want
    .option(SqlServerStreamingSource.START_TIME,"1555555555555")
    .option(SqlServerStreamingSource.TABLE_NAME,"SOMETABLE")
    .load()
    .coalesce(1)
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "put here your zookeepers")
    .option("topic", "SomeTopic")
    .option("checkpointLocation", "/some checkpoint")
    .start().awaitTermination()
}
