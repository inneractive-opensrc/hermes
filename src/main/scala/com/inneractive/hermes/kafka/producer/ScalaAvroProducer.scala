package com.inneractive.hermes.kafka.producer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getConfigs
import com.inneractive.hermes.kafka.streams.HermesConfig.setSchemaUrl
import com.inneractive.hermes.model.EventType
import com.inneractive.hermes.model.JoinEvent1
import com.inneractive.hermes.model.JoinFullEvent
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.Random
import scala.util.Try

/**
  * Created by Richard Grossman on 2017/07/25.
  */
object ScalaAvroProducer extends App with Logging {
  val countries = List("US","FR","IL","GE")

  val partial = (args.length == 2) && args(1) == "partial"

  val numRecordToSend = Try {
    args(0).toInt
  } getOrElse (throw new IllegalArgumentException("NumRecord not defined"))

  HermesConfig().fold(_.head.description, implicit c => startProducer)

  def startProducer(implicit config: HermesConfig) = {
    val c = getConfigs
    c.putAll(setSchemaUrl())
    val producer = new KafkaProducer[String, JoinFullEvent](c)

    implicit val producerJoin1 = new KafkaProducer[String, JoinEvent1](c)

    info("Start Sending now")
    val maxValues = EventType.values.length
    for (i <- 1 to numRecordToSend) {
      val aggregationEvent = makeAggregationEvent(maxValues, i)
      val record = new ProducerRecord[String, JoinFullEvent]("joinstream2", i.toString, aggregationEvent)

      if (partial) {
        if (i % 2 == 0) sendJoinEvent(i)
      }
      else sendJoinEvent(i)

      producer.send(record)
    }
  }

  def sendJoinEvent(i : Int)(implicit producer : KafkaProducer[String,JoinEvent1]) = {
    val e1 = JoinEvent1.newBuilder().setEventType(EventType.JOIN1).setSessionId(i.toString).setDimension1("dim1").setValue1(1)
    val r1 = new ProducerRecord[String, JoinEvent1]("join1", i.toString, e1.build())
    producer.send(r1)
  }

  def makeAggregationEvent(maxValue: Int, i: Int) = {
    import collection.JavaConverters._
    val randomInt = Random.nextInt(4)

    JoinFullEvent.newBuilder
      .setEventTimestamp(System.currentTimeMillis)
      .setAdNetworkId(s"network$randomInt")
      .setPublisherId(s"pub$randomInt")
      .setContentId(s"content$randomInt")
      .setCountryCode(countries(randomInt))
      .setIABCategories(List("AB1","AB2").asJava)
      .setOccurrences(1)
      .setEventType(EventType.AD_REQUEST)
      .setSessionId(i.toString)
      .setPublisherGross(1)
      .setIaGross(1)
      .setPublisherNet(1)
      .setIaNet(1)
      .build
  }

}
