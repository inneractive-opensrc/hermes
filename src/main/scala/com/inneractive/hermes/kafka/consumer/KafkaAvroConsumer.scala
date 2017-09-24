package com.inneractive.hermes.kafka.consumer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getBaseConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getConsumerConfig
import com.inneractive.hermes.model.SAggEventAd
import com.sksamuel.avro4s._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import com.ovoenergy.kafka.serialization.avro4s._
import scala.collection.JavaConverters._


/**
  * Created by Richard Grossman on 2017/07/25.
  */
object KafkaAvroConsumer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  HermesConfig().fold(_.head.description, implicit c => startConsumer)


  def startConsumer(implicit config: HermesConfig) = {
    val consumer = new KafkaConsumer[String, SAggEventAd](getConsumerConfig, new StringDeserializer(),
      avroBinarySchemaIdDeserializer[SAggEventAd](config.urls.schema, isKey = false))
    consumer.subscribe(List("scalatopic").asJava)

    implicit val SAggEventAdFromRecord = FromRecord[SAggEventAd]

    while(true) {
      val records = consumer.poll(100)
      val iter = records.asScala
      iter foreach  { v =>
        logger.info("R" + v.value().toString)
      }
    }
  }

}
