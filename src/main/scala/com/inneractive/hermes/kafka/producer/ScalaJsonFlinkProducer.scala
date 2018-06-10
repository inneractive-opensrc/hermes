package com.inneractive.hermes.kafka.producer

import java.io.File

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getBaseConfig
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by Richard Grossman on 2017/07/25.
  */
object ScalaJsonFlinkProducer extends App with Logging {

  val json =
    s"""
      {"osv":"7.0","placement_type":"Native","n":"wifi","ad_type":10,"session":"7294905427745919236","table":"sdk_event_41","event":41,"day":"2018-03-20","date_created":1521561428294,"appid":"TheHuffingtonPost_NewsStory_Android","osn":"Android","pkgn":"com.huffingtonpost.android","extra":[{"url":"http://cdn01.basis.net/113700/113625/77e40f3df7c2b0f8.mp4","bitrate":3317,"mime":"video/mp4","delivery":"progressive"}],"hour":15,"adnt":"Rubicon_Video","contentid":"599039","pkgv":"19.0.7","sdkv":"6.5.0","model":"samsung SM-G920V"}
    """.stripMargin

  val fileInput = scala.io.Source.fromFile(new File(args(0)))

  HermesConfig().fold(_.head.description, implicit c => startProducer)

  def startProducer(implicit config: HermesConfig) = {
    val c = getBaseConfig
    c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](c)

    info("Start Sending now")

    var i = 0
    for (json <- fileInput.getLines()) {
        val record = new ProducerRecord[String, String](config.topics.input, i.toString, json)
        producer.send(record)
        i = i + 1
      }

    producer.flush()
    producer.close()
  }


}
