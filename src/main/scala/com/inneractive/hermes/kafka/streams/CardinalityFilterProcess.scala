package com.inneractive.hermes.kafka.streams

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.inneractive.CliStarter
import com.inneractive.hermes.kafka.services.CreativeIdService
import com.inneractive.hermes.kafka.streams.HermesConfig.getStreamsConfig
import com.inneractive.hermes.model.JsonDeserializerNoException
import grizzled.slf4j.Logging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import org.apache.kafka.streams.kstream.{KStreamBuilder, Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import scala.util.Try

/**
  * Created by Richard Grossman on 2017/11/08.
  */
object CardinalityFilterProcess extends App with Logging with CliStarter {

  val params = parseCli(args)
  val stream = HermesConfig().fold(_.head.description, implicit c => streamProcess)

  sys.addShutdownHook {
    info("Shutdown")
    stream match {
      case (stream : KafkaStreams, restService :CreativeIdService) =>
        stream.close()
        restService.stop()
      case o: String => logger.error(o)
    }
  }

  def streamProcess(implicit config: HermesConfig) = {

    val jsonSerializer = new JsonSerializer
    val jsonDeserializer = new JsonDeserializerNoException
    val jsonSerdes = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)

    val cardinalityStoreSupplier = Stores.create("cardinalityStore")
      .withStringKeys()
      .withDoubleValues()
      .persistent()
      .build()

    val builder = new KStreamBuilder()
    builder.addStateStore(cardinalityStoreSupplier)

    val inputStream = builder.stream[String, JsonNode](Serdes.String, jsonSerdes, config.topics.aggregation)

    val correct_stream = inputStream.transform(new CardinalityTransformerSupplier, "cardinalityStore")
    correct_stream.to(Serdes.String, jsonSerdes, "CORRECT_EVENTS")

    val streams = new KafkaStreams(builder, getStreamsConfig)

    params.foreach {
      p =>
        if (p.cleanup) {
          info("Cleanup Stream apps requested")
          streams.cleanUp()
        }
    }

    streams.start()

    val storeRestAPI = new CreativeIdService(streams, config.streams.apiendpointport)
    storeRestAPI.start()

    (streams, storeRestAPI)
  }
}


class CardinalityTransformerSupplier(implicit config : HermesConfig) extends
  TransformerSupplier[String, JsonNode, KeyValue[String, JsonNode]]
{
  override def get() = new CardinalityTransformer
}

import com.inneractive.hermes.kafka.streams.KeyValueImplicits._

class CardinalityTransformer(implicit config : HermesConfig) extends Transformer[String, JsonNode, KeyValue[String, JsonNode]] with Logging {
  private var creativeIdStore: KeyValueStore[String, Double] = _
  private var totalEarned    : Double                        = 0.0
  private var totalEntries : Int = 0
  private var totalFilter : Int = 0
  private val threshold : Double = config.streams.threshold

  override def init(context: ProcessorContext) = {
    context.schedule(config.streams.punctuate)
    creativeIdStore = context.getStateStore("cardinalityStore").asInstanceOf[KeyValueStore[String, Double]]
  }

  override def punctuate(timestamp: Long) = {
    val iterator = creativeIdStore.all()

    totalEarned = 0
    while (iterator.hasNext) {
      totalEarned = totalEarned + iterator.next().value
      totalEntries = totalEntries + 1
    }

    logger.info(s"Total Earned : $totalEarned")

    creativeIdStore.flush()
    null
  }

  private def removeCreativeId(creativeId: String, json: JsonNode) = {
    logger.debug(s"${creativeId} has been removed from event")
    val jsonObject = json.asInstanceOf[ObjectNode]
    totalFilter = totalFilter + 1
    jsonObject.put("creativeId", "")
    jsonObject
  }

  override def transform(key: String, value: JsonNode) = {
    val tryCorrect = Try {
      val creativeId = value.get("creativeId").asText()
      val price = value.get("publisherGross").asDouble(0.0)

      val correctNode = if (creativeId.isEmpty) value else if (price > 0) {
        val currentValue = creativeIdStore.get(creativeId) + price
        creativeIdStore.put(creativeId, currentValue)

        if (totalEarned > 0) {
          val part = currentValue / totalEarned
          if (part < threshold) {
            logger.debug(s"CREATIVE_ID removed ==> $creativeId is $currentValue / $totalEarned = $part")
            removeCreativeId(creativeId, value)
          } else {
            logger.info(s"CREATIVE_ID OK ==> $creativeId is $currentValue / $totalEarned = $part")
            value
          }
        } else value
      } else {
        removeCreativeId(creativeId, value)
      }

      (key, correctNode)
    }

    if (tryCorrect.isFailure) logger.error("Failure occurs in Transform:", tryCorrect.failed.get)
    val r = tryCorrect.map(implicit v => (v._1, v._2)).getOrElse((key,value))
    r
  }

  override def close() = {
    logger.info("Closing StateStore")
    creativeIdStore.close()
  }
}
