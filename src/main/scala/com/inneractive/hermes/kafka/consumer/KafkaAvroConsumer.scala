package com.inneractive.hermes.kafka.consumer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getConsumerConfig
import com.ovoenergy.kafka.serialization.avro4s._
import com.sksamuel.avro4s._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
  * Created by Richard Grossman on 2017/07/25.
  */
object KafkaAvroConsumer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  HermesConfig().fold(_.head.description, implicit c => startConsumer)

  def startConsumer(implicit config: HermesConfig) = {
    implicit val valueHolder: FromRecord[ValuesHolder] = FromRecord[ValuesHolder]

    val consumer = new KafkaConsumer[String, ValuesHolder](getConsumerConfig, new StringDeserializer(),
      avroBinarySchemaIdDeserializer[ValuesHolder](config.urls.schema, isKey = false, includesFormatByte = false))
    consumer.subscribe(List("AggregatedAvro").asJava)

    while(true) {
      val records = consumer.poll(100)
      val iter = records.asScala
      iter foreach  { v =>
        logger.info("R" + v.value().toString)
      }
    }
  }

}
