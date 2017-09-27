package com.inneractive.hermes.model

import org.apache.avro.Schema
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

/**
  * Created by Richard Grossman on 2017/09/24.
  */
object MyJsonConverter extends App {

  val schemaStr =
    """
      |{
      |  "type": "record",
      |  "name": "AggregationEvent",
      |  "namespace": "com.inneractive.hermes.model",
      |  "doc": "Full event contains all the data after join",
      |  "fields": [
      |    {"name": "sessionId", "type": "long", "doc": "generated for each request used to link with impression and clicks"},
      |    {
      |     "name": "eventType",
      |     "type": {
      |     "name":"EventType",
      |     "type": "enum",
      |     "doc": "Event type is used to ",
      |     "symbols" : ["AD_REQUEST", "DEFAULT_AD", "IMPRESSION","CLICK", "NO_AD", "AD_SERVED", "DEFAULT_CLICK",
      |                  "DOWNLOAD", "FILTERED_AD", "FRAUDULENT_CLICK", "JOIN1", "JOIN2"]
      |        }
      |    },
      |    {"name": "affiliateId", "type": "int", "default": 0},
      |    {"name": "publisherId", "type": "long"},
      |    {"name": "contentId", "type": "long"},
      |    {"name": "version", "type": "string", "default": null},
      |    {"name": "countryCode", "type": "string", "default": "US"},
      |    {"name": "distributorId", "type": "long"},
      |    {"name": "adNetworkId", "type": "long"},
      |    {"name": "eventTimestamp", "type": "long"},
      |    {"name": "modelName", "type": "string", "default": null},
      |    {"name": "deviceOs", "type": "string", "default": null},
      |    {"name": "deviceOsVersion", "type": "string", "default": null},
      |    {"name": "brandName", "type": "string", "default": null},
      |    {"name": "supportsFullScreen", "type": "int", "default": 0},
      |    {"name": "requestType", "type": "int", "default": 0},
      |    {"name": "locationType", "type": "int", "default": 0},
      |    {"name": "getImpressionsRatio", "type": "int", "default": 0},
      |    {"name": "soldRatio", "type": "int", "default": 0},
      |    {"name": "publisherGross", "type": "double", "default": 0.0},
      |    {"name": "publisherNet", "type": "double", "default": 0.0},
      |    {"name": "affiliateGross", "type": "double", "default": 0.0},
      |    {"name": "affiliateNet", "type": "double", "default": 0.0},
      |    {"name": "iaGross", "type": "double", "default": 0.0},
      |    {"name": "iaNet", "type": "double", "default": 0.0},
      |    {"name": "cpc", "type": "double", "default": 0.0},
      |    {"name": "cpm", "type": "double", "default": 0.0},
      |    {"name": "cpa", "type": "double", "default": 0.0},
      |    {"name": "engineId", "type": "long", "default": 0},
      |    {"name": "campaignId", "type": "long", "default": 0},
      |    {"name": "megaCampaignId", "type": "int", "default": 0},
      |    {"name": "sdkEventHandleVesion", "type": "boolean", "default": false},
      |    {"name": "sdkEvent", "type": "boolean", "default": false},
      |    {"name": "adFormat", "type": "int", "default": 0},
      |    {"name": "seatId", "type": "string", "default": null},
      |    {"name": "deviceId", "type": "string"},
      |    {"name": "bundleId", "type": "string", "default": null},
      |    {"name": "siteUrl", "type": "string", "default": null},
      |    {"name": "adomain", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "dealId", "type": "string", "default": null},
      |    {"name": "hbAdaptor", "type": "string", "default": null},
      |    {"name": "adapterTimeout", "type": "string", "default": null},
      |    {"name": "latency", "type": "string", "default": "0"},
      |    {"name": "iABCategories", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "appName", "type": "string", "default": null},
      |    {"name": "auctionType", "type": "string", "default": null},
      |    {"name": "iabLocationType", "type": "string", "default": null},
      |    {"name": "connectionType","type": "string", "default": null},
      |    {"name": "age", "type": "string", "default": "0"},
      |    {"name": "gender", "type": "string", "default": null},
      |    {"name": "size", "type": "string", "default": null},
      |    {"name": "winnerBidderGroup", "type": "string", "default": null},
      |    {"name": "bidRequests", "type": "int", "default": 0},
      |    {"name": "bidResponses", "type": "int", "default": 0},
      |    {"name": "validBidResponses", "type": "int", "default": 0},
      |    {"name": "rtbFloorPrice", "type": "string", "default": "0.0"},
      |    {"name": "winBid", "type": "int", "default": 0},
      |    {"name": "extractedCampaignId", "type": "string", "default": null},
      |    {"name": "creativeId", "type": "string", "default": null},
      |    {"name": "language", "type": "string", "default": null},
      |    {"name": "appId", "type": "string", "default": null},
      |    {"name": "audiences", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "spotId", "type": "string", "default": null},
      |    {"name": "video", "type": "boolean", "default": false},
      |    {"name": "display", "type": "boolean", "default": true},
      |    {"name": "region", "type": "string", "default": null},
      |    {"name": "metro", "type": "string", "default": null},
      |    {"name": "city", "type": "string", "default": null},
      |    {"name": "zip", "type": "string", "default": null},
      |    {"name": "carrier", "type": "string", "default": null},
      |    {"name": "storeCategories", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "osAndVersion", "type": "string", "default": null},
      |    {"name": "correctModelName", "type": "string", "default": null},
      |    {"name": "clearPrice", "type": "double", "default": 0.0},
      |    {"name": "existsInCrossWise", "type": "boolean", "default": false},
      |    {"name": "adUnitSupportsCompanionAds", "type": "boolean", "default": false},
      |    {"name": "winningAdContainsCompanionAd", "type": "boolean", "default": false},
      |    {"name": "blockedIABCategories", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "blockedAdvertiserAdomain", "type": {"type":"array","items": "string"} , "default": []},
      |    {"name": "deviceIDType", "type": "string", "default": null},
      |    {"name": "floorPrice", "type": "double", "default": 0.0},
      |    {"name": "dnt", "type": "boolean", "default": false},
      |    {"name": "occurrences", "type": "int", "default": 0},
      |    {"name": "environment", "type": "string", "default": null},
      |    {"name": "table", "type": "string", "default": null},
      |    {"name": "hour", "type": "string", "default": null},
      |    {"name": "day", "type": "string", "default": null}
      |  ]
      |}
    """.stripMargin

  val schema = new Schema.Parser().parse(schemaStr)
  val jsonConverter = new JsonAvroConverter()

  def convert2(json : String)= {
    jsonConverter.convertToGenericDataRecord(json.getBytes, schema)
  }

  val event = convert2("{\n  \"eventType\": \"AD_REQUEST\",\n  \"sessionId\": 4949047768200141721,\n  \"affiliateId\": 0,\n  \"publisherId\": 207879,\n  \"contentId\": 633263,\n  \"version\": \"IA-JS-TAG-2.12\",\n  \"countryCode\": \"US\",\n  \"distributorId\": 559,\n  \"adNetworkId\": 1,\n  \"eventTimestamp\": 1506241903936,\n  \"modelName\": \"0\",\n  \"deviceOs\": \"Android\",\n  \"deviceOsVersion\": \"7.1\",\n  \"brandName\": \"Google\",\n  \"supportsFullScreen\": 0,\n  \"requestType\": 19,\n  \"locationType\": 3,\n  \"getImpressionsRatio\": 0,\n  \"soldRatio\": 0,\n  \"publisherGross\": 0,\n  \"publisherNet\": 0,\n  \"affiliateGross\": 0,\n  \"affiliateNet\": 0,\n  \"iaGross\": 0,\n  \"iaNet\": 0,\n  \"cpc\": 0,\n  \"cpm\": 0,\n  \"cpa\": 0,\n  \"engineId\": 0,\n  \"campaignId\": 3766,\n  \"megaCampaignId\": 0,\n  \"sdkEventHandleVesion\": false,\n  \"sdkEvent\": false,\n  \"adFormat\": 1,\n  \"seatId\": \"\",\n  \"deviceId\": \"6afe936d-bb45-4b17-92ad-2015ac0616c1\",\n  \"bundleId\": \"com.qihoo.security\",\n  \"siteUrl\": \"\",\n  \"adomain\": [],\n  \"dealId\": \"\",\n  \"hbAdaptor\": \"\",\n  \"adapterTimeout\": \"\",\n  \"latency\": \"227\",\n  \"iABCategories\": [\n    \"IAB3\"\n  ],\n  \"appName\": \"360 Security -Free Antivirus,Booster,Space Cleaner\",\n  \"auctionType\": \"Second place price\",\n  \"iabLocationType\": \"GPS/Location Services\",\n  \"connectionType\": \"WIFI\",\n  \"age\": \"\",\n  \"gender\": \"\",\n  \"size\": \"320x50\",\n  \"winnerBidderGroup\": \"\",\n  \"bidRequests\": 155,\n  \"bidResponses\": 38,\n  \"validBidResponses\": 1,\n  \"rtbFloorPrice\": \"1.0126582278481013\",\n  \"winBid\": 0,\n  \"extractedCampaignId\": \"\",\n  \"creativeId\": \"\",\n  \"language\": \"en\",\n  \"appId\": \"\",\n  \"audiences\": [],\n  \"spotId\": \"\",\n  \"video\": false,\n  \"display\": true,\n  \"region\": \"LA\",\n  \"metro\": \"612\",\n  \"city\": \"Shreveport\",\n  \"zip\": \"71106\",\n  \"carrier\": \"Comcast Cable\",\n  \"storeCategories\": [\n    \"Tools\"\n  ],\n  \"osAndVersion\": \"Android-7.1\",\n  \"correctModelName\": \"Pixel XL\",\n  \"clearPrice\": 0,\n  \"existsInCrossWise\": false,\n  \"adUnitSupportsCompanionAds\": false,\n  \"winningAdContainsCompanionAd\": false,\n  \"blockedIABCategories\": [],\n  \"blockedAdvertiserAdomain\": [],\n  \"deviceIDType\": \"aaid\",\n  \"floorPrice\": 1.0126582278481013,\n  \"dnt\": false,\n  \"occurrences\": 1,\n  \"environment\": \"ia-staging\",\n  \"table\": \"AggregationEventAd\",\n  \"hour\": \"08\",\n  \"day\": \"2017-09-24\"\n}")

  println(event)
}
