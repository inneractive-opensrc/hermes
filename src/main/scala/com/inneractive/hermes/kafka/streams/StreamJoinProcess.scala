package com.inneractive.hermes.kafka.streams

import com.inneractive.hermes.kafka.anomalies.RealTimeAnomalyDetector.parseCli
import com.inneractive.hermes.kafka.streams.HermesConfig._
import com.inneractive.hermes.model.EventType
import com.inneractive.hermes.model.JoinEvent1
import com.inneractive.hermes.model.JoinFullEvent
import grizzled.slf4j.Logging
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder


object StreamJoinProcess extends App with Logging {

  val params = parseCli(args)
  val stream = HermesConfig().fold(_.head.description, implicit c => streamProcess)

  sys.addShutdownHook {
    info("Shutdown streaming processing")
    stream match {
      case o: KafkaStreams => o.close()
      case o: String => logger.error(o)
    }
  }

  def streamProcess(implicit config: HermesConfig) = {
    val streamConfig = getConfigs
    val (serdeString, serdesEvent) = getAvroEventSerdes[JoinFullEvent]

    /**
      * Create topology builder
      */
    val builder = new KStreamBuilder()

    /**
      * Define the Stream
      */
    val left: KStream[String, JoinFullEvent] =
      builder.stream[String, JoinFullEvent](serdeString, serdesEvent, "joinstream2")

    val right: KStream[String, JoinEvent1] =
      builder.stream[String, JoinEvent1](serdeString, getAvroEventSerdes[JoinEvent1]._2, "join1")

    /**
      * Join AggregationEvent with join1Event
      */
//    val join1: KStream[String, JoinFullEvent] =
//      left.join(
//        right, (fullEvent: JoinFullEvent, join1: JoinEvent1) => valueJoiner1(fullEvent, join1),
//        JoinWindows.of(20000), serdeString, serdesEvent, getAvroEventSerdes[JoinEvent1]._2
//      )

    val join1: KStream[String, JoinFullEvent] =
      left.leftJoin(
        right, (fullEvent: JoinFullEvent, join1: JoinEvent1) => valueJoiner1(fullEvent, join1),
        JoinWindows.of(30000), serdeString, serdesEvent, getAvroEventSerdes[JoinEvent1]._2
      )

    join1.to(serdeString, serdesEvent, "unifiedEvent")

    val streams = new KafkaStreams(builder, streamConfig)

    params.foreach {
      p =>
        if (p.cleanup) {
          info("Cleanup Stream apps requested")
          streams.cleanUp()
        }
    }

    streams.start()

    streams
  }

  def valueJoiner1(joinFullEvent: JoinFullEvent, joinEvent: JoinEvent1) = {
    if (joinEvent != null) {
        joinFullEvent.setEventType(EventType.JOIN1)
        joinFullEvent.setDimension1(joinEvent.getDimension1)
        joinFullEvent.setValue1(joinEvent.getValue1)
        joinFullEvent.setIaGross(0.0)
        joinFullEvent.setIaNet(0.0)
        joinFullEvent.setPublisherGross(0.0)
        joinFullEvent.setPublisherNet(0.0)
        joinFullEvent.setOccurrences(1)
    } else logger.info(s"id:${joinFullEvent.getSessionId} Join1 Event is null")

    joinFullEvent
  }

}
