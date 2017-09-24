package kamon

import com.typesafe.config.ConfigFactory
import kamon.metric.Entity
import kamon.metric.HistogramKey
import kamon.metric.instrument.UnitOfMeasurement
import kamon.statsd.TagsMetricKeyGenerator
import org.scalatest.Matchers
import org.scalatest.WordSpec

/**
  * Created by Richard Grossman on 2017/08/02.
  */
class TagsMetricKeyGeneratorSpec extends WordSpec with Matchers {
  val defaultConfiguration = ConfigFactory.parseString(
    """
      |kamon.statsd.simple-metric-key-generator {
      |  application = kamon
      |  hostname-override = localhost
      |  include-hostname = true
      |  metric-name-normalization-strategy = normalize
      |}
    """.stripMargin
  )
    "TagsMetricKeyGeneratorSpec" should {
      "Work as SimpleMetricKeyGeneratorSpec" in {
        val metricKeyGenerator = new TagsMetricKeyGenerator(defaultConfiguration)

        val metric = HistogramKey("histo1", UnitOfMeasurement.Unknown)
        val entity = Entity("entity1", "processing-time")
        metricKeyGenerator.generateKey(entity, metric) should be("kamon.localhost.processing-time.entity1.histo1")
      }

      "Not Generate a key including tags if not configured" in {
        val metricKeyGenerator = new TagsMetricKeyGenerator(defaultConfiguration) {
          override val addTags = false
        }

        val metric = HistogramKey("histo1", UnitOfMeasurement.Unknown)
        val entity = Entity("entity1", "processing-time", Map("tag1" -> "value1"))
        metricKeyGenerator.generateKey(entity, metric) should be("kamon.localhost.processing-time.entity1.histo1")
      }

      "Generate a key including tags if configured to do so" in {
        val metricKeyGenerator = new TagsMetricKeyGenerator(defaultConfiguration) {
          override val addTags = true
        }

        val metric = HistogramKey("histo1", UnitOfMeasurement.Unknown)
        val entity = Entity("entity1", "processing-time", Map("tag1" -> "value1", "tag2" -> "value2"))
        metricKeyGenerator.generateKey(entity, metric) should be("kamon.localhost.processing-time.entity1.histo1{tag1=\"value1\",tag2=\"value2\"}")
      }
    }
}
