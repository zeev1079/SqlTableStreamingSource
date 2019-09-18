package zeev.hbase.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.{AuthUtil, ChoreService, HBaseConfiguration}
import com.typesafe.config.ConfigFactory

/**
 * @Author: Zeev Feldbeine
 * creted: 03/01/2019
 *
 */
class HbaseConfiguration {

  def getConfiguration():Configuration ={

    val config = ConfigFactory.load()
    val configuration:Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", config.getString("hbase.zkQuorum"))
    configuration.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zkClientPort"))
    configuration.set("zookeeper.znode.parent", config.getString("hbase.zkBasePath"))
    configuration.set("hbase.regionserver.kerberos.principal", config.getString("hbase.regionserverkerberosprincipal"))
    configuration.set("hbase.master.kerberos.principal", config.getString("hbase.masterkerberosprincipal"))
    UserGroupInformation.setConfiguration(configuration)

    configuration
  }

}
