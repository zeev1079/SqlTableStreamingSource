package zeev.sql.util

import java.sql.{Connection, Date, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.TimeZone

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

class SqlDAO {

  private val conn:Connection =new SqlConfiguration().getConfiguration()

  private def executeSqlQuery(sqlStatment:String,startTimeStamp:String,endTimeStamp:String):(ResultSet,PreparedStatement)={

    val stat = conn.prepareStatement(sqlStatment)
    stat.setString(1,startTimeStamp)
    stat.setString(2,endTimeStamp)
    (stat.executeQuery(),stat)
  }

  def getSqlServerResults(tableName:String,timestapName:String,indexName:String,startTime:Long,endTime:Long):(ResultSet,PreparedStatement)={

    val startTimeStamp = convertEpochToTimestamp(startTime)
    val endTimeStamp = convertEpochToTimestamp(endTime)
    val sqlStatment:String = "SELECT * FROM " + tableName + " WHERE "+timestapName+" BETWEEN ? AND ? ORDER BY "+timestapName+" asc"
    executeSqlQuery(sqlStatment,startTimeStamp,endTimeStamp)
  }


  private def convertEpochToTimestamp(epoch:Long):String={
    if(epoch < 0)
      return null
    val date = new Date(epoch)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))
    format.format(date)
  }

  def closeConnections():Unit={

    conn.close()
  }

}
