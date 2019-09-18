package zeev.hbase.util

import java.util.concurrent.ConcurrentMap

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import zeev.sql.util.SqlDAO
import org.apache.hadoop.hbase.filter.{CompareFilter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.json.JSONObject
import org.json4s.NoTypeHints
import org.json4s.native.Serialization

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

class HbaseDAO {

  private val connection =  ConnectionFactory.createConnection(new HbaseConfiguration().getConfiguration())
  private val offsetTable = connection.getTable(TableName.valueOf("Offsets_Manager"))
  private val  END_ROW = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

  def getLatestOffsets(topicName:String, groupId:String, defaultStartTime:Long): (Long,Long,Long,Long,Long) =
  {
    val scan = new Scan()
    scan.setReversed(true)
    scan.setStopRow(Bytes.toBytes(topicName + "|" + groupId + "|"))
    scan.setStartRow(Bytes.toBytes(topicName + "|" + groupId + "|"+END_ROW))
    scan.setMaxResultSize(1)
    val resultsItr = offsetTable.getScanner(scan).iterator()
    if(resultsItr==null || !resultsItr.hasNext)
      return (-1,-1,-1,-1,defaultStartTime)
    val result = resultsItr.next()
    (Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("endCurrentOffset"))).toLong
      ,Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("startOffset"))).toLong
      ,Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("endOffset"))).toLong
      ,Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("lastOffsetCommitted"))).toLong
      ,Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("endWindow"))).toLong)
  }

  def saveOffsetResult(topicName:String,groupId:String,startCurrentOffset:Long,endCurrentOffset:Long,
                       startOffset:Long,endOffset:Long,lastOffsetCommitted:Long,
                       startWindow:Long,endWindow:Long)={
    val put = new Put(Bytes.toBytes(topicName + "|" + groupId + "|" + addLeadingZeros(endCurrentOffset)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("startCurrentOffset"),Bytes.toBytes(String.valueOf(startCurrentOffset)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("endCurrentOffset"),Bytes.toBytes(String.valueOf(endCurrentOffset)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("startWindow"),Bytes.toBytes(String.valueOf(startWindow)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("endWindow"),Bytes.toBytes(String.valueOf(endWindow)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("startOffset"),Bytes.toBytes(String.valueOf(startOffset)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("endOffset"),Bytes.toBytes(String.valueOf(endOffset)))
    put.addColumn(Bytes.toBytes("metadata"),Bytes.toBytes("lastOffsetCommitted"),Bytes.toBytes(String.valueOf(lastOffsetCommitted)))

    try{
      offsetTable.put(put)
    }catch {
      case ex:Exception => {
        ex.printStackTrace()
        throw ex.getCause
      }
    }
  }

  def retrieveOffsets(topicName:String, groupId:String, offset:Long, dataValues:ConcurrentMap[Long,String], sqlServerDao:SqlDAO,
                      table_name:String, timestamp_param_name:String, index_param_name:String, lastOffset:Long):Unit={

    val startRow = Bytes.toBytes(topicName + "|" + groupId + "|"+addLeadingZeros(offset))
    val endRow = Bytes.toBytes(topicName + "|" + groupId + "|" +END_ROW)
    val filter = new FilterList()
    filter.addFilter(new SingleColumnValueFilter(Bytes.toBytes("metadata"),Bytes.toBytes("startCurrentOffset"),CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(String.valueOf(offset))))
    filter.addFilter(new SingleColumnValueFilter(Bytes.toBytes("metadata"),Bytes.toBytes("endCurrentOffset"),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(String.valueOf(offset))))
    val scan = new Scan()
    scan.setStartRow(startRow)
    scan.setStopRow(endRow)
    scan.setFilter(filter)
    val resultsItr = offsetTable.getScanner(scan).iterator()

    while(resultsItr.hasNext)
    {
      val resultTempNext =resultsItr.next()
      if((Bytes.toString(resultTempNext.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("startCurrentOffset"))).toLong<=offset && Bytes.toString(resultTempNext.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("endCurrentOffset"))).toLong>=offset))
      {
        var startCurrentOffset:Long = Bytes.toString(resultTempNext.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("startCurrentOffset"))).toLong
        val endTime = Bytes.toString(resultTempNext.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("endWindow"))).toLong
        val startTime = Bytes.toString(resultTempNext.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("startWindow"))).toLong
        val (results,stmt) = sqlServerDao.getSqlServerResults(table_name,timestamp_param_name,index_param_name,startTime,endTime)
        if(results != null)
        {
          val json:JSONObject  = new JSONObject()
          implicit val formats = Serialization.formats(NoTypeHints)
          val resultsMetadata = results.getMetaData
          val columnSize = resultsMetadata.getColumnCount
          while(results.next())
          {
            for(index <- 1 to columnSize)
            {
              json.put(resultsMetadata.getColumnName(index),results.getObject(index))
            }
            if(startCurrentOffset >= lastOffset)
              dataValues.putIfAbsent(startCurrentOffset + 1L,json.toString)
            startCurrentOffset = startCurrentOffset + 1L
          }
        }
        if(results!=null)
          results.close()
        if(stmt!=null)
          stmt.close()
      }
    }

  }

  def close():Unit={
    offsetTable.close()
    connection.close()
  }

  private def addLeadingZeros(value:Long):String={
    val maxSize = 20
    val temp = String.valueOf(value)
    StringUtils.leftPad(temp, maxSize, "0")
  }

}
