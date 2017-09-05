package com.example.remote

import java.sql.Timestamp

import com.example.model.CustomPacket
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime

import scala.util.matching.Regex

object PacketAnalyzer extends App{

  val sparkSession = SparkSession
    .builder()
    .appName("PacketsAnalyzer2")
    .enableHiveSupport()
    .getOrCreate()

	val sparkContext = sparkSession.sparkContext

  val ssc = new StreamingContext(sparkContext, Seconds(10))

	val sqlContext = sparkSession.sqlContext

//  packets.saveAsTextFiles("/tasks/task9/testoutput/")
//	val df = sqlContext.createDataFrame(packets., schema)
//  val packets = ssc.socketTextStream("10.0.2.2", 8585, StorageLevel.MEMORY_AND_DISK_2)
	val schema = StructType(
		Seq(
			StructField("host", DataTypes.StringType, nullable = false),
			StructField("size", DataTypes.IntegerType, nullable = false),
			StructField("speed", DataTypes.DoubleType, nullable = false),
			StructField("timestamp", DataTypes.TimestampType, nullable = false)
		))

	ssc
		.receiverStream(new PacketsReceiver("10.0.2.2", 8585))
		.map(strToCustomPacket)
  	.map(packet => (packet.destAdrr, packet))
		.window(Minutes(5))
		.reduceByKey(mergePackets)
  	.map(tuple => getStats(tuple._2))
		.map(stat => Seq(stat.host, stat.size, stat.speed, stat.timestamp))
  	.foreachRDD(rdd => {
			val df = sqlContext.createDataFrame(rdd.map(Row(_:_*)), schema)
			df.write.mode("append").saveAsTable("stats")
		})
//	val settings1 = sqlContext.table("settings_v1").toDF()
//	val func: (String, String) => String = {
//
//	}
	ssc.start()
  ssc.awaitTermination()

	def strToCustomPacket(str: String): CustomPacket = {
		PacketString.unapply(str).getOrElse(throw new Exception(s"Can't parse $str"))
	}

	def mergePackets(pckt1: CustomPacket, pckt2: CustomPacket): CustomPacket =
		/*pckt1 match {
		case CustomPacket(adr, size, time) if time.isBefore(pckt2.time) =>
			CustomPacket(adr, size+pckt2.size, time)
		case CustomPacket(adr, size, time) if time.isAfter(pckt2.time) =>
			CustomPacket(adr, size+pckt2.size, pckt2.time)*/
	if(pckt1.time.isBefore(pckt2.time)){
		CustomPacket(pckt1.destAdrr, pckt1.size+pckt2.size, pckt1.time)
	} else CustomPacket(pckt1.destAdrr, pckt1.size+pckt2.size, pckt2.time)

	def getStats(packet: CustomPacket): Stats = {
		val currTime = DateTime.now
		Stats(
			packet.destAdrr,
			packet.size,
			packet.size/ org.joda.time.Seconds
				.secondsBetween(packet.time, currTime).getSeconds,
			new Timestamp(currTime.getMillis)
		)
	}
}
object PacketString{
	val PacketRegex: Regex = "CustomPacket\\((.*),(.*),(.*)\\)".r
	def unapply(arg: String): Option[CustomPacket] = arg match {
		case PacketRegex(host, size, time) =>
			Some(CustomPacket(host, size.toInt, DateTime.parse(time)))
		case _ => None
	}
}
case class Stats(host: String, size: Int, speed: Double, timestamp: Timestamp)
//remember about 10.0.2.2