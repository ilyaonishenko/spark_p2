package com.example.local

import java.util.{Collections, Properties}
import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._
class KafkaCons{

	private val topic = "don_alarmos"
	val props = createConsumerConfig("127.0.0.1:2181", topic)
	val consumer = new KafkaConsumer[String, String](props)

	def run(): Unit = {
		consumer.subscribe(Collections.singletonList(this.topic))

		Executors.newSingleThreadExecutor.execute( new Runnable {
			override def run(): Unit = {
				while (true) {
					val records = consumer.poll(1000)
					for (record <- records) {
//						System.out.println(s"Received message: $record")
						System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
					}
				}
			}
		})
	}

//	def run() = {
//		consumer.subscribe(Collections.singletonList(this.topic))
//
//		Executors.newSingleThreadExecutor.execute(    new Runnable {
//			override def run(): Unit = {
//				while (true) {
//					val records = consumer.poll(1000)
//
//					for (record <- records) {
//						System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
//					}
//				}
//			}
//		})


	def createConsumerConfig(brokers: String, groupId: String): Properties = {
		val props = new Properties()
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
		props
	}
}

object KafkaCons extends App{
	val consumer = new KafkaCons
	consumer.run()
}
