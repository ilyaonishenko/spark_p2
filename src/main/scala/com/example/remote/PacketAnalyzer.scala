package com.example.remote

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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

  val ssc = new StreamingContext(sparkContext, Seconds(10))

//  val packets = ssc.receiverStream(new PacketsReceiver("localhost", 9993))
  val packets = ssc.socketTextStream("localhost", 10016, StorageLevel.MEMORY_AND_DISK_2)
  packets.map(println).saveAsTextFiles("/tasks/task9/testoutput/")


	ssc.start()
  ssc.awaitTermination()
}
