package zeev.spark.streaming.inputpartitions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import zeev.spark.streaming.partitionreaders.SqlServerPartitionReader

import scala.collection.mutable.ListBuffer

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

/**
 * An input partition returned by {@link DataSourceReader#planInputPartitions()} and is
 * responsible for creating the actual data reader of one RDD partition.
 * The relationship between {@link InputPartition} and {@link InputPartitionReader}
 * is similar to the relationship between {@link Iterable} and {@link java.util.Iterator}.
 *
 * Note that {@link InputPartition}s will be serialized and sent to executors, then
 * {@link InputPartitionReader}s will be created on executors to do the actual reading. So
 * {@link InputPartition} must be serializable while {@link InputPartitionReader} doesn't need to
 * be.
 */

class SqlServerStreamInputPartition(SqlServerDataList:ListBuffer[String])
  extends InputPartition[InternalRow] {
  override def createPartitionReader() = new SqlServerPartitionReader(SqlServerDataList)
}
