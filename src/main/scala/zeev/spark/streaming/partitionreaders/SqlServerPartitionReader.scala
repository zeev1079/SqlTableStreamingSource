package zeev.spark.streaming.partitionreaders

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

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
class SqlServerPartitionReader(SqlServerDataList:ListBuffer[String]) extends InputPartitionReader[InternalRow] {
  private var currentIdx = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIdx += 1
    currentIdx < SqlServerDataList.size
  }

  override def get(): InternalRow  = {
    val message = SqlServerDataList(currentIdx)
    InternalRow(UTF8String.fromString(message))
  }// want to have Row(topicId,messageTime,CurrentTime,message.asJson)

  override def close(): Unit = {}//need some implimentation to do this
}
