package com.inneractive.hermes.kafka.producer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getBaseConfig
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Random, Try}

/**
  * Created by Richard Grossman on 2017/07/25.
  */
object ScalaJsonProducer extends App with Logging {

  val countries = List("US", "FR", "IL", "GE")

  val jsonVast =
    s"""
       | {
       |  "sessionId": #SessionID#,
       |  "vastinfo": #SessionID#,
       |  "eventTimestamp": #Timestamp#
       | }
     """.stripMargin

  val json =
    s"""
       |{
       |  "eventType": "AD_REQUEST",
       |  "sessionId": #SessionID#,
       |  "affiliateId": 0,
       |  "publisherId": 206698,
       |  "contentId": 600772,
       |  "version": "Sm2m-2.1.0",
       |  "countryCode": "HN",
       |  "distributorId": 642,
       |  "adNetworkId": 1,
       |  "eventTimestamp": #Timestamp#,
       |  "modelName": "0",
       |  "nokiaSeries": 0,
       |  "deviceOs": "iOS",
       |  "deviceOsVersion": "11.0",
       |  "brandName": "Apple",
       |  "supportsFullScreen": 1,
       |  "requestType": 107,
       |  "locationType": 3,
       |  "getImpressionsRatio": 0,
       |  "soldRatio": 0,
       |  "publisherGross": #Gross#,
       |  "publisherNet": 0,
       |  "affiliateGross": 0,
       |  "affiliateNet": 0,
       |  "iaGross": 0,
       |  "iaNet": 0,
       |  "cpc": 0,
       |  "cpm": 0,
       |  "cpa": 0,
       |  "engineId": 0,
       |  "campaignId": 3766,
       |  "megaCampaignId": 0,
       |  "sdkEventHandleVesion": false,
       |  "sdkEvent": false,
       |  "adFormat": 1,
       |  "seatId": "",
       |  "deviceId": "1E26DFD7-23AD-4C22-8FA5-B7D7317D655D",
       |  "bundleId": "1160535565",
       |  "siteUrl": "",
       |  "adomain": [],
       |  "dealId": "",
       |  "hbAdaptor": "",
       |  "adapterTimeout": "",
       |  "latency": "4",
       |  "iABCategories": [
       |    "IAB14",
       |    "IAB18"
       |  ],
       |  "appName": "Starfire for Clash Royale",
       |  "auctionType": "Second place price",
       |  "iabLocationType": "IP Address",
       |  "connectionType": "3G",
       |  "age": "",
       |  "gender": "",
       |  "size": "320x480",
       |  "winnerBidderGroup": "",
       |  "bidRequests": 17,
       |  "bidResponses": 1,
       |  "validBidResponses": 0,
       |  "rtbFloorPrice": "0.06849315068493152",
       |  "winBid": 0,
       |  "extractedCampaignId": "",
       |  "creativeId": "#CreativeId#",
       |  "language": "en",
       |  "appId": "",
       |  "audiences": [],
       |  "spotId": "",
       |  "video": false,
       |  "display": false,
       |  "region": "FM",
       |  "metro": "",
       |  "city": "Talanga",
       |  "zip": "",
       |  "carrier": "Hondutel",
       |  "storeCategories": [
       |    "Social Networking"
       |  ],
       |  "osAndVersion": "iOS-11.0",
       |  "correctModelName": "iPhone",
       |  "clearPrice": 0,
       |  "existsInCrossWise": false,
       |  "adUnitSupportsCompanionAds": false,
       |  "winningAdContainsCompanionAd": false,
       |  "blockedIABCategories": [
       |    "IAB25-6",
       |    "IAB25-5",
       |    "IAB25-4",
       |    "IAB25-3",
       |    "IAB25-2",
       |    "IAB25-1",
       |    "IAB26"
       |  ],
       |  "blockedAdvertiserAdomain": [],
       |  "deviceIDType": "idfa",
       |  "floorPrice": 0.06849315068493152,
       |  "dnt": false,
       |  "occurrences": 1,
       |  "environment": "ia-staging",
       |  "table": "AggregationEventAd",
       |  "hour": "08",
       |  "day": "2017-10-04"
       |}
    """.stripMargin

  val numRecordToSend = Try {
    args(0).toInt
  } getOrElse (throw new IllegalArgumentException("NumRecord not defined"))

  HermesConfig().fold(_.head.description, implicit c => startProducer)

  def startProducer(implicit config: HermesConfig) = {
    val c = getBaseConfig
    c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](c)
    val vastProducer = new KafkaProducer[String, String](c)

    info("Start Sending now")
    for (i <- 1 to numRecordToSend) {
      Seq(("creative1", "10"), ("creative2", "1"), ("creative3", "0.1")) foreach { case (creative, price) =>

        val sessionId = Random.nextLong().toString
        val timestamp = System.currentTimeMillis()
        val futureTimeStamp = timestamp // + (1000 * 60 * 2)

        val newJson = json.replace("#CreativeId#", creative)
          .replace("#Gross#", price)
          .replace("#SessionID#", sessionId)
          .replace("#Timestamp#", timestamp.toString)
        val record = new ProducerRecord[String, String](config.topics.input, sessionId, newJson)

        val vastJson = jsonVast
          .replaceAll("#SessionID#", sessionId)
          .replace("#Timestamp#", futureTimeStamp.toString)
        val vastRecord = new ProducerRecord[String,String]("VAST-TOPIC", sessionId, vastJson)

        producer.send(record)
        vastProducer.send(vastRecord)
      }
    }

    producer.flush()
    producer.close()

    vastProducer.flush()
    vastProducer.close()
  }


}
