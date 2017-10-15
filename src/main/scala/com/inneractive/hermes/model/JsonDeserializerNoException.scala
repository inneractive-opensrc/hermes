package com.inneractive.hermes.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import grizzled.slf4j.Logging
import java.util
import org.apache.kafka.common.serialization.Deserializer
import scala.util.Try

/**
  * Created by Richard Grossman on 2017/10/15.
  */
class JsonDeserializerNoException extends Deserializer[JsonNode] with Logging {
  private val objectMapper = new ObjectMapper

  override def configure(map: util.Map[String, _], b: Boolean) = { }

  override def close() = { }

  override def deserialize(s: String, bytes: Array[Byte]) = (Option(bytes) map { b =>
    Try { this.objectMapper.readTree(b) } getOrElse {
      logger.error("Failed to deserialize Json")
      null
    }
  }).orNull
}
