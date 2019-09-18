package zeev.spark.streaming.batchreaders

import java.sql.{PreparedStatement, ResultSet}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.StructType
import org.json.JSONObject
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import zeev.hbase.util.HbaseDAO
import zeev.spark.streaming.inputpartitions.SqlServerStreamInputPartition
import zeev.spark.streaming.offsets.SqlOffset
import zeev.spark.streaming.sources.SqlServerStreamingSource
import zeev.sql.util.SqlDAO

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

class SqlServerStreamingMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader
{
  private val table_name = options.get(SqlServerStreamingSource.TABLE_NAME).orElse("defaultTable")
  private val group_name = options.get(SqlServerStreamingSource.GROUP_NAME).orElse("GroupName")
  private val timestamp_param_name = options.get(SqlServerStreamingSource.TIMESTAMP_PARAMTER_NAME).orElse("TSColumns")
  private val index_param_name = options.get(SqlServerStreamingSource.INDEX_PARAMTER_NAME).orElse("SomeIdColumns")
  private val numPartitions = options.get(SqlServerStreamingSource.NUM_PARTITIONS).orElse("100").toInt
  private val window_size = options.get(SqlServerStreamingSource.WINDOW_SIZE).orElse("1 minutes").toLowerCase
  private val window_jump = options.get(SqlServerStreamingSource.WINDOW_JUMP).orElse("1 minutes").toLowerCase
  private val window_delay = options.get(SqlServerStreamingSource.DELAY).orElse("5 minutes").toLowerCase

  private val window_sizeTuple = getWindowValue(window_size)
  private val window_jumpTuple = getWindowValue(window_jump)
  private val window_delayTuple = getWindowValue(window_delay)


  val current_time: String = Instant.now().toEpochMilli.toString
  private val startTime = options.get(SqlServerStreamingSource.START_TIME).orElse(current_time).toLong


  @GuardedBy("this")
  private val hbaseDao:HbaseDAO = new HbaseDAO()
  @GuardedBy("this")
  private var sqlServerDao:SqlDAO = _

  private var stopped:Boolean=false
  private var workerPutInConcurentList:Thread = _

  @GuardedBy("this")
  private val sqlServerDataMap:ConcurrentMap[Long,String] = new ConcurrentHashMap[Long,String]()

  private val defaultOffsetVals = hbaseDao.getLatestOffsets(table_name,group_name,startTime)

  private val NO_DATA_OFFSET = SqlOffset(-1)

  private var startOffset: SqlOffset = new SqlOffset(defaultOffsetVals._2)

  private var endOffset: SqlOffset = new SqlOffset(defaultOffsetVals._3)

  @GuardedBy("this")
  private var currentOffset: SqlOffset = new SqlOffset(defaultOffsetVals._1)
  @GuardedBy("this")
  private var lastOffsetCommitted : SqlOffset = new SqlOffset(defaultOffsetVals._4)
  @GuardedBy("this")
  private var startTimeOffset: SqlOffset =  new SqlOffset(defaultOffsetVals._5)


  queryFromSql()

  private def queryFromSql():Unit=
  {
    sqlServerDao = new SqlDAO()
    workerPutInConcurentList = new Thread("SqlStreamReader")
    {
      setDaemon(true)
      override def run(): Unit = {
        streamFromSqlTable()
      }
    }
    workerPutInConcurentList.start()
  }

  private def streamFromSqlTable(): Unit =
  {
    var startTimeStampInstance:Instant = Instant.ofEpochMilli(startTimeOffset.offset)
    var endTimeStamp:Long = startTimeStampInstance.plus(window_sizeTuple._1, window_sizeTuple._2).toEpochMilli
    val json:JSONObject  = new JSONObject()
    while(!stopped)
    {
      this.synchronized
      {
        val (results:ResultSet,stat:PreparedStatement) = sqlServerDao.getSqlServerResults(table_name,timestamp_param_name,index_param_name,startTimeOffset.offset,endTimeStamp)
        val previousOffset:Long = currentOffset.offset
        if(results != null)
        {
          implicit val formats = Serialization.formats(NoTypeHints)
          val resultsMetadata = results.getMetaData
          val columnSize = resultsMetadata.getColumnCount
          while(results.next())
          {
            for(index <- 1 to columnSize)
            {
              json.put(resultsMetadata.getColumnName(index),results.getObject(index))
            }
            sqlServerDataMap.putIfAbsent(currentOffset.offset+1L,json.toString)
            currentOffset = currentOffset+1L
          }
        }
        try{
          if(results!=null)
            results.close()
          if(stat!=null)
          {
            stat.clearParameters()
            stat.clearBatch()
            stat.close()
          }

        }catch {
          case ex:Exception=>{
            println(s"the error from sql: ${ex.getMessage}")
            throw ex
          }
        }
        hbaseDao.saveOffsetResult(table_name,group_name,previousOffset,currentOffset.offset, startOffset.offset,endOffset.offset,lastOffsetCommitted.offset, startTimeOffset.offset,endTimeStamp)
        startTimeOffset = new SqlOffset(Instant.ofEpochMilli(startTimeOffset.offset).plus(window_jumpTuple._1,window_jumpTuple._2).toEpochMilli)

        endTimeStamp = Instant.ofEpochMilli(startTimeOffset.offset).plus(window_sizeTuple._1, window_sizeTuple._2).toEpochMilli
      }
      while(Instant.now().isBefore( Instant.ofEpochMilli(endTimeStamp).plus(window_delayTuple._1,window_delayTuple._2) ) )
      {
        Thread.sleep(100)
      }
    }
  }

  override def setOffsetRange(start: Optional[Offset],
                              end: Optional[Offset]): Unit =  {
    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[SqlOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[SqlOffset]
  }

  override def getStartOffset(): Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def getEndOffset(): Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] =  {
    val startOrdinal = SqlOffset.convert(startOffset).get.offset.toLong + 1L
    val endOrdinal = SqlOffset.convert(endOffset).get.offset.toLong + 1L

    var newBlocks:ListBuffer[String] = new ListBuffer[String]()
    for(index <- startOrdinal until endOrdinal) {
      var temp = ""
      if (sqlServerDataMap.containsKey(index.toLong))
        temp = sqlServerDataMap.get(index.toLong)
      else if(lastOffsetCommitted.offset!= -1)
        hbaseDao.retrieveOffsets(table_name, group_name, index.toLong,
          sqlServerDataMap, sqlServerDao, table_name, timestamp_param_name, index_param_name,lastOffsetCommitted.offset)
      newBlocks+=sqlServerDataMap.get(index.toLong)
    }
    newBlocks.grouped(numPartitions).map { block =>
      new SqlServerStreamInputPartition(block).asInstanceOf[InputPartition[InternalRow]]
    }.toList.asJava
  }

  override def commit(end: Offset): Unit =  {

    val newOffset = SqlOffset.convert(end).getOrElse(
      sys.error(s"MicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toLong

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    (lastOffsetCommitted.offset until newOffset.offset).foreach(
      index =>
        if(sqlServerDataMap.containsKey(index+1L)) sqlServerDataMap.remove(index+1L)
    )
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = {
    stopped = true
    sqlServerDao.closeConnections()
    hbaseDao.close()
  }


  override def deserializeOffset(json: String): Offset = {
    SqlOffset(json.toLong)
  }

  override def readSchema(): StructType = SqlServerStreamingSource.SCHEMA


  private def getWindowValue(jump:String):(Long,ChronoUnit)={
    val valuesTime = jump.trim.split(" ")
    val unitTime:ChronoUnit = valuesTime(1).toLowerCase match {
      case "second"|"seconds" => ChronoUnit.SECONDS
      case "minute"|"minutes" => ChronoUnit.MINUTES
      case "hour" | "hours" => ChronoUnit.HOURS
      case _ =>  ChronoUnit.MINUTES
    }
    (valuesTime(0).toLong,unitTime)
  }

}