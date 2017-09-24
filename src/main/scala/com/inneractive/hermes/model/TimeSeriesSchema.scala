package com.inneractive.hermes.model

import org.apache.avro.Schema
import org.apache.kafka.connect.data.SchemaBuilder.float64
import org.apache.kafka.connect.data.SchemaBuilder.int64
import org.apache.kafka.connect.data.SchemaBuilder.struct

/**
  * Created by Richard Grossman on 2017/09/06.
  */
object TimeSeriesSchema {
  val fieldTimestamp   = "timestamp"
  val fieldMetricValue = "metricValue"

  private val connectSchema: org.apache.kafka.connect.data.Schema = struct()
    .field(fieldTimestamp, int64().required().doc("timestamp the metric to build multiple time series"))
    .field(fieldMetricValue, float64().required().doc("Value of the metric in the time series"))
    .build().schema()

  val schema: Schema = new org.apache.avro.Schema.Parser().parse(connectSchema.toString)
}

case class TimeSeriesEntry(timestamp: Long, value: Double)

