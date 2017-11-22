package com.inneractive.hermes.kafka.anomalies

import com.inneractive.hermes.kafka.TimeSeriesProvider
import com.inneractive.hermes.kafka.myType.TimeSeries
import java.util.Properties
import org.joda.time.DateTime
import scala.collection.JavaConverters._

/**
  * Created by Richard Grossman on 2017/08/30.
  */
object EgadsAnomalyDetector {
  def detectAnomalies(timeSeriesProvider: TimeSeriesProvider, properties: Properties): TimeSeries = {
    val inputProcessor = new SentinelInputProcessor(timeSeriesProvider)

    inputProcessor.processInput(properties)
    val anoms = inputProcessor.result flatMap (_.intervals.asScala)
    anoms.map(a => (new DateTime(a.startTime * 1000), a.actualVal.toDouble)).toVector
  }

}
