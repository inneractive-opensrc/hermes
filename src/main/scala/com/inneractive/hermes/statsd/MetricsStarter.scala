package com.inneractive.hermes.statsd

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import org.joda.time.format.DateTimeFormat

/**
  * Created by Richard Grossman on 2017/08/01.
  */
object MetricsStarter extends App {

  val config = ConfigFactory.defaultApplication()
  val csvDateFormat = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC()

  Kamon.start()

  val histo1 = Kamon.metrics.histogram("histo1", Map("tag1" -> "value1", "tag2" -> "value2"))
  val histo2 = Kamon.metrics.histogram("histo2")

  def record(startAt : Long, value : Long, period : Long) : Boolean =
    if (System.currentTimeMillis() -  startAt > period) true
    else {
      histo1.record(value)
      histo2.record(value)
      record(startAt, value, period)
    }

  def log(timestamp : Long, value : Long, maxPeriod : Int, current : Int) : Long =
    if (current == maxPeriod) timestamp
    else {
      val datetime = csvDateFormat.print(timestamp)
      println(s"$datetime,$value")
      histo1.record(value)
      log(timestamp + 120000, value, maxPeriod, current + 1)
    }


  record(System.currentTimeMillis(), 10, 1000 * 10000)
  record(System.currentTimeMillis(), 1000, 1000 * 2)
  record(System.currentTimeMillis(), 10, 1000 * 10000)

  Kamon.shutdown()
}
