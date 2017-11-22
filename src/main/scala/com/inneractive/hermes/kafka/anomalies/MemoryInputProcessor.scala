package com.inneractive.hermes.kafka.anomalies

import com.yahoo.egads.control.ProcessableObjectFactory
import com.yahoo.egads.data.TimeSeries
import com.yahoo.egads.utilities.InputProcessor
import java.util.Properties

/**
  * Created by Richard Grossman on 2017/08/30.
  */
class MemoryInputProcessor(timeSeries : Seq[TimeSeries]) extends InputProcessor {
  override def processInput(properties: Properties) = {
    timeSeries.foreach {entry =>
      val po = ProcessableObjectFactory.create(entry, properties)
      po.process()
    }
  }
}
