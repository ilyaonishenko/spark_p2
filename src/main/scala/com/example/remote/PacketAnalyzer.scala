package com.example.remote

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PacketAnalyzer extends App{

  val warehouseLocation = new File("/spark-warehouse").getAbsolutePath

  val sparkContext = SparkSession
    .builder()
    .appName("PacketsAnalyzer")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
    .sparkContext

  val ssc = new StreamingContext(sparkContext, Seconds(1))

  val packets = ssc.socketTextStream("localhost", 9999)
  packets.foreachRDD(rdd => rdd.foreach(str => println(str)))
}
