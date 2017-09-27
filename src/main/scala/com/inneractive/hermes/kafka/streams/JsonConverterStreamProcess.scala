package com.inneractive.hermes.kafka.streams

import com.inneractive.hermes.kafka.anomalies.RealTimeAnomalyDetector.parseCli
import com.inneractive.hermes.kafka.streams.HermesConfig.getAvroEventSerdes
import com.inneractive.hermes.kafka.streams.HermesConfig.getConfigs
import com.inneractive.hermes.kafka.streams.HermesConfig.getGenericSerdes
import com.inneractive.hermes.model.JoinFullEvent
import com.inneractive.hermes.model.MyJsonConverter
import grizzled.slf4j.Logging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder

/**
  * Created by Richard Grossman on 2017/09/24.
  */
object JsonConverterStreamProcess extends App with Logging {
  val params = parseCli(args)
  val stream = HermesConfig().fold(_.head.description, implicit c => streamProcess)

  sys.addShutdownHook {
    info("Shutdown streaming processing")
    stream match {
      case o: KafkaStreams => o.close()
      case o: String => logger.error(o)
    }
  }

  import KeyValueImplicits._

  def streamProcess(implicit config: HermesConfig) = {
    val streamConfig = getConfigs
    val (serdeString, _) = getAvroEventSerdes[JoinFullEvent]
    val (_, avroValueSerde) = getGenericSerdes

    val builder = new KStreamBuilder()

    val jsonStream: KStream[String, String] =
      builder.stream[String, String](serdeString, serdeString, "AGGREGATION_EVENT_JSON")

    val avroStream : KStream[String, GenericRecord] = jsonStream.map{(_,v) =>
      val record = MyJsonConverter.convert2(v)
      val sessionId = String.valueOf(record.get("sessionId"))
      (sessionId, record)
    }

    avroStream.to(serdeString, avroValueSerde, "AGGREGATION_EVENT_AVRO")

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

}
