package com.inneractive.hermes.kafka.producer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getConfigs
import com.inneractive.hermes.kafka.streams.HermesConfig.setSchemaUrl
import com.inneractive.hermes.model.EventType
import com.inneractive.hermes.model.JoinEvent1
import com.inneractive.hermes.model.JoinEvent2
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

  val numRecordToSend = Try {
    args(0).toInt
  } getOrElse (throw new IllegalArgumentException("NumRecord not defined"))

  HermesConfig().fold(_.head.description, implicit c => startProducer)

  def startProducer(implicit config: HermesConfig) = {
    val c = getConfigs
    c.putAll(setSchemaUrl())
    val producer = new KafkaProducer[String, JoinFullEvent](c)
    val producerJoin1 = new KafkaProducer[String, JoinEvent1](c)
    val producerJoin2 = new KafkaProducer[String, JoinEvent2](c)

    info("Start Sending now")
    val maxValues = EventType.values.length
    for (i <- 1 to numRecordToSend) {
      val aggregationEvent = makeAggregationEvent(maxValues, i)
      val record = new ProducerRecord[String, JoinFullEvent]("joinstream2", i.toString, aggregationEvent)

      val e1 = JoinEvent1.newBuilder().setEventType(EventType.JOIN1).setSessionId(i.toString).setDimension1("dim1").setValue1(1)
      val e2 = JoinEvent2.newBuilder().setEventType(EventType.JOIN2).setSessionId(i.toString).setDimension2("dim2").setValue2(2)

      val r1 = new ProducerRecord[String, JoinEvent1]("join1", i.toString, e1.build())
      val r2 = new ProducerRecord[String, JoinEvent2]("join2", i.toString, e2.build())

      producer.send(record)

      producerJoin1.send(r1)
      producerJoin2.send(r2)
    }
  }

  def makeAggregationEvent(maxValue: Int, i: Int) = {
    import collection.JavaConverters._
    val eventType = EventType.values()(Random.nextInt(maxValue))
    val randomInt = Random.nextInt(4)

    JoinFullEvent.newBuilder
      .setEventTimestamp(System.currentTimeMillis)
      .setAdNetworkId(s"network$randomInt")
      .setPublisherId(s"pub$randomInt")
      .setContentId(s"content$randomInt")
      .setCountryCode(countries(randomInt))
      .setIABCategories(List("AB1","AB2").asJava)
      .setOccurrences(1)
      .setEventType(eventType)
      .setSessionId(i.toString)
      .setPublisherGross(1)
      .setIaGross(1)
      .setPublisherNet(1)
      .setIaNet(1)
      .build
  }

}
