package com.fyber.benchmark

import java.io.{File, FileOutputStream}

import com.inneractive.hermes.model.{EventType, JoinFullEvent}
import com.inneractive.hermes.utils.SnappyCodec
import org.xerial.snappy.SnappyOutputStream

import scala.util.Random

object AvroJson extends App {

  def makeAggregationEvent(maxValue: Int, i: Int) = {
    val countries = List("US", "FR", "IL", "GE")

    import collection.JavaConverters._
    val eventType = EventType.values()(Random.nextInt(maxValue))
    val randomInt = Random.nextInt(4)

    JoinFullEvent.newBuilder
      .setEventTimestamp(System.currentTimeMillis)
      .setAdNetworkId(s"network$randomInt")
      .setPublisherId(s"pub$randomInt")
      .setContentId(s"content$randomInt")
      .setCountryCode(countries(randomInt))
      .setIABCategories(List("AB1", "AB2").asJava)
      .setOccurrences(1)
      .setEventType(eventType)
      .setSessionId(i.toString)
      .setPublisherGross(1)
      .setIaGross(1)
      .setPublisherNet(1)
      .setIaNet(1)
      .build
  }

  val file = new File("/tmp/Json.snappy")
  val outStream = new FileOutputStream(file)
  

  val snappyOut = new SnappyOutputStream(outStream)

  for (i <- 1 to args(0).toInt) {
    outStream.write(makeAggregationEvent(5, 1).toByteBuffer.array())
  }
  outStream.flush()
  outStream.close()

  snappyOut.write(makeAggregationEvent(5, 1).toByteBuffer.array())

  snappyOut.flush()
  snappyOut.close()

  val snappyCodec = new SnappyCodec()

  val dd = snappyCodec.read("/tmp/Json.snappy")

  println("decompressed:" +  dd.length)
  println(new String(dd))

}
