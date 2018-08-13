package com.xerago.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.log4j.{Level, LogManager, Logger}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark
import org.apache.spark.sql.cassandra
/*import org.apache.spark.sql.SparkSession*/

object SparkCassandra extends App {
  case class Albums(title: String, country: String, performer:String, quality:String,status:String,year:Int)


  override def main(args: Array[String]): Unit = {


    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host","10.50.255.247")
      .set("spark.cassandra.auth.username", "root")
      .set("spark.cassandra.auth.password", "xerago@999")
      .setAppName("CassandraSpark").setMaster("local[*]")
      .set("spark.cassandra.connection.port", "9042")
    log.warn("Hello demo")
    val sc = new SparkContext(conf)
    val rdd = sc.cassandraTable("cvm", "customers")
    val file_collect=rdd.collect()
    log.warn("I am done")
    file_collect.map(println(_))

  }
}
