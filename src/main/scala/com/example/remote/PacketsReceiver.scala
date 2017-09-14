package com.example.remote

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.example.model.CustomPacket
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import com.example.remote.PacketAnalyzer.{sparkSession, strToCustomPacket}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

class PacketsReceiver(host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
    with Logging {

	var state: Map[String, (Int, Double)] = Map()

  override def onStart(): Unit =
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()

  override def onStop(): Unit = {}

  def receive(): Unit = {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        store(userInput)
				alarmController(maybeAlarmWithMap)(userInput)
				userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }

	def alarmController(maybeAlarm: String => Option[String])(str: String): Unit = {
		println(s"in alarm controller with str: $str")
		maybeAlarm(str) match {
			case Some(string) => KafkarProd(string)
			case None => KafkarProd("No alarms and no surprises")
		}
	}

	def maybeAlarm(packetInfo: String): Option[String] ={
		import sparkSession.sql
		val packet = strToCustomPacket(packetInfo)
		val pattern = "select * from stats1 where host = \""
		Try(sql(pattern + packet.destAdrr + "\"")) match {
			case s: Success[DataFrame] =>
				val maxSize = s.value.rdd.map(row => row.getInt(1)).max
				if(packet.size > maxSize) Some("Size limit!")
				else Option.empty
			case f: Failure[DataFrame] =>
				f.exception.printStackTrace()
				Option.empty
		}
	}

	def maybeAlarmWithMap(packetInfo: String): Option[String] = {
		val packet = strToCustomPacket(packetInfo)
		println(s"mybealarmwithmap with packet: $packet")
		state.get(packet.destAdrr) match {
			case Some((size, _)) if size < packet.size =>
				state = state.updated(packet.destAdrr, (packet.size, 0))
				println("first")
				Some("size < packet.size\n\n")
			case Some((size, _)) if size > packet.size =>
				println("second")
				Some("size > packet.size\n\n")
			case Some((_ , _)) =>
				println("third")
				Some("I do not know what")
			case None =>
				println("None")
				state = state.updated(packet.destAdrr, (packet.size, 0))
				Option.empty
		}
	}
}

object KafkarProd {
	val props = new Properties()
	props.put("bootstrap.servers", "localhost:6667")
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

	val topic = "don_alarmos"
	val key = "key"

	def producer = new KafkaProducer[String, String](props)

	def apply(msg: String,
						topic: String = this.topic,
						key: String = this.key): Unit = {
		val producer = this.producer
		println(s"we are going to send thsi: $msg")
		producer.send(new ProducerRecord[String, String](topic, key, msg))
		println("and we send it")
		producer.close()
	}
}
