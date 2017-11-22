package com.inneractive.hermes.kafka.anomalies

import com.inneractive.hermes.kafka.TimeSeriesProvider
import com.yahoo.egads.control.ProcessableObjectFactory
import com.yahoo.egads.data.Anomaly
import com.yahoo.egads.data.TimeSeries
import com.yahoo.egads.utilities.InputProcessor
import java.util
import java.util.Properties
import scala.collection.JavaConverters._

/**
  * Created by Richard Grossman on 2017/08/30.
  */
class SentinelInputProcessor(timeSeriesProvider: TimeSeriesProvider) extends InputProcessor {
  var result : Seq[Anomaly] = Seq.empty[Anomaly]

  override def processInput(properties: Properties) = {
    timeSeriesProvider.populate() foreach { timeSeries =>

      val timestamps = timeSeries.unzip._1 map (time => time.getMillis / 1000)
      val values = timeSeries.unzip._2 map (value => value.toFloat)
      val ts = new TimeSeries(timestamps.toArray, values.toArray)

      val po = ProcessableObjectFactory.create(ts, properties)
      po.process()

      result = po.result().asInstanceOf[util.ArrayList[Anomaly]].asScala
    }
  }

}
