package com.inneractive.hermes.kafka

import io.confluent.connect.avro.AvroConverterConfig
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import java.util
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.storage.Converter
import com.ovoenergy.kafka.serialization.core._


/**
  * Created by Richard Grossman on 2017/07/27.
  */
class ScalaAvroConverter extends Converter {
  private var isKey : Boolean = false
  private var schemaRegistry : CachedSchemaRegistryClient = _
  private var kafkaSerializer : KafkaAvroSerializer = _
  private var kafkaDeserializer : KafkaAvroDeserializer = _

  override def configure(config: util.Map[String, _], isKey: Boolean): Unit = {
    this.isKey = isKey
    val avroConverterConfig = new AvroConverterConfig(config)
    schemaRegistry = new CachedSchemaRegistryClient(avroConverterConfig.getSchemaRegistryUrls,
      avroConverterConfig.getMaxSchemasPerSubject)
    kafkaSerializer = new KafkaAvroSerializer(schemaRegistry)
    kafkaSerializer.configure(config, isKey)

    kafkaDeserializer = new KafkaAvroDeserializer(schemaRegistry, config)
    kafkaDeserializer.configure(config, isKey)
  }

  override def fromConnectData(topic: String, schema: Schema, o: scala.Any): Array[Byte] = {
    Array.emptyByteArray
  }

  override def toConnectData(
    s: String,
    bytes: Array[Byte]
  ): SchemaAndValue = ???
}
