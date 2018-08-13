package com.xerago.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark
import org.apache.spark.sql.cassandra
/*import org.apache.spark.sql.SparkSession*/

object SparkCassandra extends App {
  case class Albums(title: String, country: String, performer:String, quality:String,status:String,year:Int)


  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host","Ipaddress")
      .set("spark.cassandra.auth.username", "1234")
      .set("spark.cassandra.auth.password", "12345")
      .setAppName("CassandraSpark").setMaster("local[*]")
      .set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    val rdd = sc.cassandraTable("cvm", "customers")
    val file_collect=rdd.collect()
    file_collect.map(println(_))

  }
}
