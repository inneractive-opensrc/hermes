package com.inneractive.hermes.kafka.streams

import com.fasterxml.jackson.databind.JsonNode
import com.inneractive.hermes.kafka.anomalies.RealTimeAnomalyDetector.parseCli
import com.inneractive.hermes.kafka.streams.HermesConfig.getConfigs
import com.inneractive.hermes.model.JsonDeserializerNoException
import com.inneractive.hermes.model.ValueHolderDeserializer
import com.inneractive.hermes.model.ValueHolderSerializer
import com.inneractive.hermes.utils.WindowedSerde
import com.sksamuel.avro4s.ValuesHolder
import grizzled.slf4j.Logging
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Reducer
import org.apache.kafka.streams.kstream.TimeWindows

/**
  * Created by Richard Grossman on 2017/10/10.
  */
object AggregationProcess extends App with Logging {

  val aggregationKeys = List(
    Vector("eventType", "publisherId", "contentId"),
    Vector("eventType", "publisherId", "adNetworkId", "countryCode")
  )
  val doubleFields    = List("publisherGross", "publisherNet", "iaGross", "iaNet")

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
    import scala.collection.JavaConverters._

    val jsonSerializer = new JsonSerializer
    val jsonDeserializer = new JsonDeserializerNoException
    val jsonSerdes = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)

    val valueHolderSerdes = Serdes.serdeFrom(new ValueHolderSerializer, new ValueHolderDeserializer)

    val streamConfig = getConfigs
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    val builder = new KStreamBuilder()

    /**
      * Define the Stream
      */
    val inputStream: KStream[String, JsonNode] = builder.stream[String, JsonNode](
      Serdes.String,
      jsonSerdes,
      "jsontopic"
    )

    val keyedStream: KStream[String, ValuesHolder] = inputStream
      .filter((_, v) => v != null)
      .flatMap { (key, node) =>
      val keysValues = aggregationKeys.map { keys =>
        val key = keys.map(k => node.get(k).asText()).mkString("#")
        val valuesHolder = getValues(node)
        KeyValue.pair[String, ValuesHolder](key, valuesHolder)
      }

      keysValues.asJava
    }

    val windowedSerdes = new WindowedSerde[String](Serdes.String())
    windowedSerdes.configure(HermesConfig.setSchemaUrl("false"), true)

    keyedStream
      .groupByKey(Serdes.String(), valueHolderSerdes)
      .reduce(new SimpleReducer, TimeWindows.of(TimeUnit.SECONDS.toMillis(1)))
      .to(windowedSerdes, valueHolderSerdes, "AggregatedAvro")

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

  def getValues(v: JsonNode): ValuesHolder = {
    val doubles = doubleFields map (f => v.get(f).asDouble())
    val occurrence = Array(v.get("occurrences").asInt())
    ValuesHolder(doubles.toArray, occurrence)
  }
}


class SimpleReducer() extends Reducer[ValuesHolder] {
  override def apply(value1: ValuesHolder, value2: ValuesHolder) = value1 + value2
}