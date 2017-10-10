package com.inneractive.hermes.kafka.producer

import com.inneractive.hermes.kafka.streams.HermesConfig
import com.inneractive.hermes.kafka.streams.HermesConfig.getBaseConfig
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Try

/**
  * Created by Richard Grossman on 2017/07/25.
  */
object ScalaJsonProducer extends App with Logging {
  val countries = List("US", "FR", "IL", "GE")
  val json = "{\n  \"eventType\": \"AD_REQUEST\",\n  \"sessionId\": 6827857674743869672,\n  \"affiliateId\": 0,\n  \"publisherId\": 206698,\n  \"contentId\": 600772,\n  \"version\": \"Sm2m-2.1.0\",\n  \"countryCode\": \"HN\",\n  \"distributorId\": 642,\n  \"adNetworkId\": 1,\n  \"eventTimestamp\": 1507105355106,\n  \"modelName\": \"0\",\n  \"nokiaSeries\": 0,\n  \"deviceOs\": \"iOS\",\n  \"deviceOsVersion\": \"11.0\",\n  \"brandName\": \"Apple\",\n  \"supportsFullScreen\": 1,\n  \"requestType\": 107,\n  \"locationType\": 3,\n  \"getImpressionsRatio\": 0,\n  \"soldRatio\": 0,\n  \"publisherGross\": 1.0,\n  \"publisherNet\": 0,\n  \"affiliateGross\": 0,\n  \"affiliateNet\": 0,\n  \"iaGross\": 0,\n  \"iaNet\": 0,\n  \"cpc\": 0,\n  \"cpm\": 0,\n  \"cpa\": 0,\n  \"engineId\": 0,\n  \"campaignId\": 3766,\n  \"megaCampaignId\": 0,\n  \"sdkEventHandleVesion\": false,\n  \"sdkEvent\": false,\n  \"adFormat\": 1,\n  \"seatId\": \"\",\n  \"deviceId\": \"1E26DFD7-23AD-4C22-8FA5-B7D7317D655D\",\n  \"bundleId\": \"1160535565\",\n  \"siteUrl\": \"\",\n  \"adomain\": [],\n  \"dealId\": \"\",\n  \"hbAdaptor\": \"\",\n  \"adapterTimeout\": \"\",\n  \"latency\": \"4\",\n  \"iABCategories\": [\n    \"IAB14\",\n    \"IAB18\"\n  ],\n  \"appName\": \"Starfire for Clash Royale\",\n  \"auctionType\": \"Second place price\",\n  \"iabLocationType\": \"IP Address\",\n  \"connectionType\": \"3G\",\n  \"age\": \"\",\n  \"gender\": \"\",\n  \"size\": \"320x480\",\n  \"winnerBidderGroup\": \"\",\n  \"bidRequests\": 17,\n  \"bidResponses\": 1,\n  \"validBidResponses\": 0,\n  \"rtbFloorPrice\": \"0.06849315068493152\",\n  \"winBid\": 0,\n  \"extractedCampaignId\": \"\",\n  \"creativeId\": \"\",\n  \"language\": \"en\",\n  \"appId\": \"\",\n  \"audiences\": [],\n  \"spotId\": \"\",\n  \"video\": false,\n  \"display\": false,\n  \"region\": \"FM\",\n  \"metro\": \"\",\n  \"city\": \"Talanga\",\n  \"zip\": \"\",\n  \"carrier\": \"Hondutel\",\n  \"storeCategories\": [\n    \"Social Networking\"\n  ],\n  \"osAndVersion\": \"iOS-11.0\",\n  \"correctModelName\": \"iPhone\",\n  \"clearPrice\": 0,\n  \"existsInCrossWise\": false,\n  \"adUnitSupportsCompanionAds\": false,\n  \"winningAdContainsCompanionAd\": false,\n  \"blockedIABCategories\": [\n    \"IAB25-6\",\n    \"IAB25-5\",\n    \"IAB25-4\",\n    \"IAB25-3\",\n    \"IAB25-2\",\n    \"IAB25-1\",\n    \"IAB26\"\n  ],\n  \"blockedAdvertiserAdomain\": [],\n  \"deviceIDType\": \"idfa\",\n  \"floorPrice\": 0.06849315068493152,\n  \"dnt\": false,\n  \"occurrences\": 1,\n  \"environment\": \"ia-staging\",\n  \"table\": \"AggregationEventAd\",\n  \"hour\": \"08\",\n  \"day\": \"2017-10-04\"\n}"

  val numRecordToSend = Try {
    args(0).toInt
  } getOrElse (throw new IllegalArgumentException("NumRecord not defined"))

  HermesConfig().fold(_.head.description, implicit c => startProducer)


  def startProducer(implicit config: HermesConfig) = {
    val c = getBaseConfig
    c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](c)

    info("Start Sending now")
    for (i <- 1 to numRecordToSend) {
      val record = new ProducerRecord[String, String]("jsontopic", i.toString, json)
      producer.send(record)
    }

    producer.flush()
    producer.close()
  }


}
