package com.inneractive.hermes.kafka.streams

import com.inneractive.hermes.model.JoinFullEvent
import com.inneractive.hermes.utils.GenericAvroSerde
import com.inneractive.hermes.utils.SpecificAvroSerde
import com.inneractive.hermes.utils.SpecificAvroSerializer
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util
import java.util.Properties
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import pureconfig.error.ConfigReaderFailures
import pureconfig.loadConfig


/**
  * Created by Richard Grossman on 2017/06/21.
  */
case class EndpointURL(brokers : String, schema : String, zookeeper : String)
case class Topics(input : String, output: String, aggregation : String)
case class Streams(punctuate : Long, threshold : Double, apiendpointhost : String, apiendpointport : Int)
case class HermesProducerConfig(ack : Int)
case class KafkaConsumerConfig(readfrom : String, groupid : String,
  autocomit: String, autocomitinterval : String, sessiontimeout : String, maxpollrequest : String)

case class HermesConfig(urls : EndpointURL, topics : Topics,
                        producerconf : HermesProducerConfig, consumerconf : KafkaConsumerConfig, streams : Streams)


object HermesConfig {
  def apply(): Either[ConfigReaderFailures, HermesConfig] = {
    val config = ConfigFactory.load()
    loadConfig[HermesConfig](config)
  }

  def getBaseConfig(implicit c: HermesConfig): Properties = {
    val streamsConfiguration = new Properties
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "events-aggregator")
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, c.urls.brokers)
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, c.urls.zookeeper)

    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, c.urls.schema)

    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, c.consumerconf.readfrom)
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")

    if (! c.streams.apiendpointhost.isEmpty) {
      streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, s"${c.streams.apiendpointhost}:${c.streams.apiendpointport}")
    }

    streamsConfiguration

  }

  def getStreamsConfig(implicit c: HermesConfig): Properties = {
    val config = getBaseConfig
    config.put("session.timeout.ms", c.consumerconf.sessiontimeout)
    config.put("max.poll.records", c.consumerconf.maxpollrequest)
     config
  }

  def getConsumerConfig(implicit c: HermesConfig): Properties = {
    val config = getBaseConfig
    config.put("group.id", c.consumerconf.groupid)
    config.put("enable.auto.commit", c.consumerconf.autocomit)
    config.put("auto.commit.interval.ms", c.consumerconf.autocomitinterval)
    config.put("session.timeout.ms", c.consumerconf.sessiontimeout)
    config.put("max.poll.records", c.consumerconf.maxpollrequest)
    config
  }

  def getConfigs(implicit c: HermesConfig): Properties = {
    val streamsConfiguration = getBaseConfig

    streamsConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    streamsConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SpecificAvroSerializer[JoinFullEvent]] )

    streamsConfiguration
  }

  def setSchemaUrl(isSpecificAvro : String = "true")(implicit c:HermesConfig) = {
    val map = new util.HashMap[String, String]()
    map.put("schema.registry.url", c.urls.schema)
    map.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, isSpecificAvro)
    map
  }

  def getAvroEventSerdes[T <: SpecificRecord](implicit  c: HermesConfig): (Serde[String], SpecificAvroSerde[T]) = {
    val stringSerde: Serde[String] = Serdes.String()

    val eventSerdes = new SpecificAvroSerde[T]()
    eventSerdes.configure(setSchemaUrl(), false)

    (stringSerde, eventSerdes)
  }

  def getGenericSerdes(implicit  c: HermesConfig): (GenericAvroSerde, GenericAvroSerde) = {
    val keySerdes = new GenericAvroSerde()
    keySerdes.configure(setSchemaUrl(isSpecificAvro = "false"), true)

    val valueSerdes = new GenericAvroSerde()
    valueSerdes.configure(setSchemaUrl(isSpecificAvro = "false"), false)

    (keySerdes, valueSerdes)
  }

  def getSchemaRegistry(implicit c: HermesConfig) = new CachedSchemaRegistryClient(c.urls.schema, 100)
}
