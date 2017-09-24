package com.inneractive.hermes.kafka.anomalies

import com.inneractive.hermes.model.TimeSeriesEntry
import com.inneractive.sentinel.anomalies.EgadsAnomalyDetector
import com.inneractive.sentinel.timeseries.TimeSeriesProvider
import com.inneractive.sentinel.timeseries.myType.TimeSeries
import com.sksamuel.avro4s.RecordFormat
import grizzled.slf4j.Logging
import java.io.File
import java.io.PrintWriter
import java.util
import java.util.Properties
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.KeyValueStore
import org.joda.time.DateTime
import scala.collection.JavaConverters._

/**
  * Created by Richard Grossman on 2017/08/06.
  */
class AnomalyDetectorProcessor(properties: Properties) extends AbstractProcessor[String, TimeSeriesEntry] with Logging {

  private var metricStore: KeyValueStore[Long, Double] = _
  val schemaAnomaly = RecordFormat[Anomaly]

  /**
    * Process the incoming element in the stream
    * Add the value received into the data store to be processed after
    *
    * @param key message key
    * @param v   message the generic record of the stream
    */
  override def process(key: String, v: TimeSeriesEntry): Unit = {
    info(s"Store Data $key ==> ${v.timestamp}-${v.value} from partition ${context().partition()}")
    for (i <- 1 to 100) {
      metricStore.put(v.timestamp, v.value)
    }
  }

  override def close(): Unit = {
    metricStore.close()
    super.close()
  }

  /**
    * Process at specific interval the KeyValueStore to evaluate the time series
    *
    * @param timestamp
    */
  override def punctuate(timestamp: Long): Unit = {
    info(s"Entry in Store ${metricStore.name()} = ${metricStore.approximateNumEntries()}")

    if (metricStore.approximateNumEntries() > 100) {
      val timeSeries = KeyValueTSProvider(metricStore.all())

      val anomalies = EgadsAnomalyDetector.detectAnomalies(timeSeries, properties)

      anomalies foreach { anoms =>
        val anom = Anomaly(anoms._1.getMillis, anoms._2)
        info(s"Found Anomaly ${anom.timestamp},${anom.value}")
        val record = schemaAnomaly.to(anom)
        context().forward(anoms._1.getMillis.toString, record)
      }

      context().commit()
    }
  }

  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    context.schedule(1000)
    metricStore = context.getStateStore("AnomalyStore").asInstanceOf[KeyValueStore[Long, Double]]
    info(s"apps : $context)")
  }
}

/**
  * Build a TimeSeries based on KeyValueIterator
  *
  * @param iterator on the StateStore
  */
case class KeyValueTSProvider(iterator: KeyValueIterator[Long, Double]) extends TimeSeriesProvider with Logging {

  def writeToFile(ts: TimeSeries) = {
    val file = new File("/tmp/kafka/ts.csv")
    val printer = new PrintWriter(file)

    ts foreach (v => printer.println(s"${v._1.getMillis},${v._2}"))
    printer.flush()
    printer.close()
  }

  def populate(): Option[TimeSeries] = {

    var firstTimeStamp: Long = -1
    val ts: java.util.ArrayList[(DateTime, Double)] = new util.ArrayList[(DateTime, Double)]()

    while (iterator.hasNext) {
      val entry = iterator.next()
      if (firstTimeStamp == -1) {
        firstTimeStamp = entry.key
      } else {
        firstTimeStamp = firstTimeStamp + 1200000
      }

      ts.add((new DateTime(firstTimeStamp), entry.value))
    }

    info(s"Detection of timeseries size : ${ts.size()}")

    iterator.close()

    Some(ts.asScala.toVector)
  }

}