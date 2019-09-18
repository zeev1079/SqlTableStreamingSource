package zeev.sql.util

import java.sql.{Connection, DriverManager}

import com.typesafe.config.ConfigFactory

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */

class SqlConfiguration {

  def getConfiguration():Connection = {
    val config = ConfigFactory.load()
    Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance()
    val serverName = config.getString("sqlserver.servername")
    val databaseName = config.getString("sqlserver.databasename")
    val instanceName = config.getString("sqlserver.instancename")
    val userName = config.getString("sqlserver.username")
    val password = config.getString("sqlserver.passwrod")
    val jdbc:String = "jdbc:jtds:sqlserver://"+serverName+";DatabaseName="+databaseName+";Instance="+instanceName+";"
    val conn:Connection = DriverManager.getConnection(jdbc,userName,password)
    conn
  }

}
