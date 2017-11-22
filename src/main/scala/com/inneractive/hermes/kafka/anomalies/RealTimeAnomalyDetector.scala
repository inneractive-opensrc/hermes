package com.inneractive.hermes.kafka.anomalies

import com.inneractive.CliStarter
import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getConfigs
import com.inneractive.hermes.model.TimeSeriesEntry
import grizzled.slf4j.Logging
import java.util.Properties
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.Stores


/**
  * Created by Richard Grossman on 2017/08/03.
  */
object RealTimeAnomalyDetector extends App with CliStarter with Logging {
  import com.inneractive.hermes.kafka.streams.KeyValueImplicits._

  val params = parseCli(args)
  val stream = HermesConfig().fold(_.head.description, implicit c => streamProcess)

  sys.addShutdownHook {
    info("Shutdown")
    stream match {
      case o : KafkaStreams => o.close()
      case o : String => logger.error(o)
    }
  }

  def streamProcess(implicit config: HermesConfig) = {
    val streamConfig = getConfigs
    val (keySerdes, valueSerdes) = HermesConfig.getGenericSerdes

    val anomalyStore = Stores.create("AnomalyStore")
      .withLongKeys()
      .withDoubleValues()
      .inMemory()
      .build()

    val processor = new AnomalyDetectorSupplier
    val builder = new KStreamBuilder()
    builder.addProcessor("anomalyProcessor", processor)
           .addStateStore(anomalyStore, "anomalyProcessor")
           .addSink("SINK", "anomalyDetected", new StringSerializer(), valueSerdes.serializer(), "anomalyProcessor")

    val inputStream = builder.stream[GenericRecord, GenericRecord](keySerdes, valueSerdes, config.topics.input)

    //val filteredStream = inputStream.filter ((key, value) => value.get("name").toString.contains("hermes.histogram.histo1"))

    val metricStream: KStream[String, TimeSeriesEntry] = inputStream.map { (k, v) =>

      val metricValue = v.get("value").asInstanceOf[Double]
      val metricName = v.get("name").toString
      val timeStamp = v.get("timestamp").asInstanceOf[Long]

      (metricName, TimeSeriesEntry(timeStamp, metricValue))
    }

    metricStream.process(processor, "AnomalyStore")

    val streams = new KafkaStreams(builder, streamConfig)

    params.foreach{
      p => if (p.cleanup) {
        info("Cleanup Stream apps requested")
        streams.cleanUp()
      }
    }

    streams.start()

    streams
  }
}

class AnomalyDetectorSupplier extends ProcessorSupplier[String, TimeSeriesEntry] {
  val egadsProps = {
    val is = this.getClass.getResourceAsStream("/egads-config.ini")
    val props = new Properties()
    props.load(is)
    props
  }

  override def get(): Processor[String, TimeSeriesEntry] = new AnomalyDetectorProcessor(egadsProps)
}
