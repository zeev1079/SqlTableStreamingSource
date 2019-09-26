# SqlTableStreamingSource

## Upcoming Medium tutorial... 

Spark2.4 custom Spark streaming from any Sql table to Kafka. 
The project is based on Apache spark for the micro Batch streaming, and HBase to store and retrieve the offsets.

## Project sturcture

 * build.sbt - dependencies of the following:
     * Hbase-client,sqlserver(change it to any sql connection), org.json, spark-sql, spark-core, spark-sql-kafka-0-10
 * src/resources/application.congf - sql server jdbc configurations, hbase connection configurations and etc
 * src/main/zeev/hbase/util - contains the logic for the HBase Configuration connection and for storing and extracting the offsets to keep the streaming fault tollorent
 * src/main/zeev/sql/util - contains the logic for the sql configurations and connections, including dao to query the sql tables
 * src/main/zeev/producers - basic Spark2.4 struct streaming to readStream from the custom sql table source, and writeStream to Kafka
 * src/main/zeev/spark/streaming - contains all the objects and classes(MicroBatchReader, DataReaderFactories, DataReader, DataReaderFactory, Custom Offsets, and etc  ) to support and implement a custom Spark2.4 sturct streaming. The following is more detail explanation:
     1. src/main/zeev/spark/streaming/offsets - this custom implementation fo the super trait of v2.reader.streaming.Offset, it is essentially an abstract representation of progress through the microBatchReader, or continuosReader. During execution the offsets will be logged and used as restart checkpoints. this will also be used to store into HBase as well.
     2. src/main/spark/streaming/sources - it will contain the custom micro batch readers classes and companion objects, the classes will extend DataSourceV2, MicroBatchReadSupport, and DataSourceRegister. this will be starting point and the class that the kafka producer is referring in the ReadStream.
     3. src/main/zeev/spark/streaming/batchreaders - implementation of the microBatchReader, it contains all the logic of getting data from sql tables, storing the offsets, restarting from checkpoints, and creating RDDs or DataReaderFactories for spark
     4. src/main/zeev/spark/streaming/partitionreaders - will contain all the custom inputpartitionReader(in spark2.3 it used to be called DataReaderFactory), and its is responsible for creating the actual data reader of one RDD partition
     5. src/main/zeev/spark/streaming/inputpartitions - its is InputPartition(used to be called in 2.3 DataReaderFactory), and its responsible for creating the actual partition readers.


## Getting Started

 1. change the src/main/resource/application.conf based on your HBase settings and sql settings
 2. in the src/main/zeev/kafka/producers - modify the kafka brokers and any input you want to add to the microBatchReader source
 3. set and define your checkpoint location
 4. run/deploy it in anyway you want, standalone, cluster mode...


### Installing/usage
1. install and import the sbt libaries


## Built With
Apache Spark, Scala, Hbase, Sql, HDFS

## Authors
Zeev Feldbeine

## License
This project is licensed under the MIT License - see the (LICENSE.md) file for details

