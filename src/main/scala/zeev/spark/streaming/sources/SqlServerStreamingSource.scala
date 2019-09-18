package zeev.spark.streaming.sources
import java.util.Optional
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types._

import zeev.spark.streaming.batchreaders.SqlServerStreamingMicroBatchReader

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

/**
 * The base interface for data source v2. Implementations must have a public, 0-arg constructor.
 *
 * Note that this is an empty interface. Data source implementations should mix-in at least one of
 * the plug-in interfaces like {@link ReadSupport} and {@link WriteSupport}. Otherwise it's just
 * a dummy data source which is un-readable/writable.
 */

class SqlServerStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister{
  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions) = new SqlServerStreamingMicroBatchReader(options)

  override def shortName() = "SqlServerStreamingSource"
}

object SqlServerStreamingSource
{
  val TABLE_NAME = "tableName"
  val GROUP_NAME = "groupName"
  val TIMESTAMP_PARAMTER_NAME = "timestampParamterName"
  val INDEX_PARAMTER_NAME = "indexParamterName"
  val NUM_PARTITIONS = "numPartitions"
  val WINDOW_SIZE = "windowSize"
  val WINDOW_JUMP= "windowJump"
  val DELAY = "delay"
  val START_TIME = "startTime"
  val PREVIOUS_IDS = "previousIds"

  //can easly include also the server configs, hence can be every sql server

  val SCHEMA2 = new StructType().add("values","string")
  val SCHEMA =
    StructType(
      StructField("values", StringType) :: Nil)
  val SCHEMA3 = new StructType().add("values","int")
}







