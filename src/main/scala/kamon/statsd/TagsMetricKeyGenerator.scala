package kamon.statsd

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import kamon.metric.Entity
import kamon.metric.MetricKey

/**
  * Created by Richard Grossman on 2017/08/02.
  */
class TagsMetricKeyGenerator(config : Config) extends MetricKeyGenerator {
  val simpleMetricKeyGenerator = new SimpleMetricKeyGenerator(config)

  val configSettings: Config = config.getConfig("kamon.statsd.tags-metric-key-generator")

  val fallback = ConfigFactory.parseString("kamon.statsd.tags-metric-key-generator.include-tags = false")
  val addTags = configSettings.withFallback(fallback).getBoolean("include-tags")

  override def generateKey(entity: Entity, metricKey: MetricKey): String = {
    val key = simpleMetricKeyGenerator.generateKey(entity, metricKey)
    if (addTags) {
      val tags = entity.tags.map{case (k,v) => k + "=\"" + v + "\""} mkString ","
      s"$key{$tags}"
    } else key
  }
}
