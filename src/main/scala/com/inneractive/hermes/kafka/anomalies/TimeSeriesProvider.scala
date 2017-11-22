package com.inneractive.hermes.kafka

import com.inneractive.hermes.kafka.myType.TimeSeries
import org.joda.time.DateTime


package object myType {
  type TS = (DateTime, Double)
  type TimeSeries = Vector[TS]
}

trait TimeSeriesProvider {
  def populate(): Option[TimeSeries]
}

case class AnomalyResult(timestamp : Long, value : Double)





