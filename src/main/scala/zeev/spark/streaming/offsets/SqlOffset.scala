package zeev.spark.streaming.offsets
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2
import org.json4s.DefaultFormats

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

/**
 * Comments from docs
 * An abstract representation of progress through a {@link MicroBatchReader} or
 * {@link ContinuousReader}.
 * During execution, offsets provided by the data source implementation will be logged and used as
 * restart checkpoints. Each source should provide an offset implementation which the source can use
 * to reconstruct a position in the stream up to which data has been seen/processed.
 *
 * Note: This class currently extends {@link org.apache.spark.sql.execution.streaming.Offset} to
 * maintain compatibility with DataSource V1 APIs. This extension will be removed once we
 * get rid of V1 completely.
 */
case class SqlOffset(offset:Long) extends v2.reader.streaming.Offset{
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  override val json = offset.toString

  def +(increment: Long) = new SqlOffset(offset + increment)
  def -(decrement: Long) = new SqlOffset(offset - decrement)
  def overrideValue(newOffset:Long) = new SqlOffset(newOffset)

}

object SqlOffset
{
  def apply(offset: SerializedOffset) : SqlOffset = new SqlOffset(offset.json.toLong)
  def convert(offset: Offset): Option[SqlOffset] = offset match {
    case lo: SqlOffset => Some(lo)
    case so: SerializedOffset => Some(SqlOffset(so))
    case _ => None
  }
}
