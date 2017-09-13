package com.example.remote

import java.sql.Timestamp
import java.util.Properties

import com.example.model.CustomPacket
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}
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
	import sparkSession.sql

	val schema = StructType(
		Seq(
			StructField("host", DataTypes.StringType, nullable = false),
			StructField("size", DataTypes.IntegerType, nullable = false),
			StructField("speed", DataTypes.DoubleType, nullable = false),
			StructField("timestamps", DataTypes.TimestampType, nullable = false)
		))

	val pattern = "select * from stats where stats1.host = \""

	val props = new Properties()
	props.put("bootstrap.servers", "127.0.0.1:2181")
	props.put("client.id", "ScalaProducerExample")
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	props.put("auto.create.topics.enable", "true")

	val producer = new KafkaProducer[String, String](props)

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
			val df1 = df.toDF()
			df1.foreach(row => {
				Try(sql(pattern + row.getString(0)+"\";")) match{
					case s: Success[DataFrame] =>
						val oldRow = s.value.reduce((row1, row2) => (row1, row2) match{
							case (row_1, row_2) if row_1.getInt(1) > row_2.getInt(1) =>
								println(s"comparing two rows: $row_1 and $row_2")
								Row(
									row_1.getString(0),
									row_1.getInt(1),
									row_1.getDouble(2),
									row_1.getTimestamp(3)
								)
							case (row_1, row_2) if row_1.getInt(1) < row_2.getInt(1) =>
								println(s"comparing2 two rows: $row_1 and $row_2")
								Row(
									row_1.getString(0),
									row_2.getInt(1),
									row_1.getDouble(2),
									row_2.getTimestamp(3)
								)
						})
						if (oldRow.getInt(1) > row.getInt(1)) {
							println("aldsadsadjsalkdsajdlksadjlksajdsalkdsadjsdsadaswwwwwwwwwwwwwwwwwwwwwwwsadmsad")
							producer.send(new ProducerRecord[String, String]("don_alarmos", "127.0.0.1", "Size warning!"))
						}
						case f: Failure[DataFrame] => println("there are some exceptions occured!")
				}
			})
			df.write.mode("append").saveAsTable("stats1")
		})
//	val settings1 = sqlContext.table("settings_v1").toDF()
//	val func: (String, String) => String = {
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
			packet.size/Math.abs(packet.time.getMillis - currTime.getMillis),
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
