package com.inneractive.hermes.model

/**
  * Created by Richard Grossman on 2017/07/25.
  */
trait SAggregationEvent {
  val eventType: EventType
  val sessionId: String
  val deviceOs : String
}

case class SAggEventAd(
  eventType: EventType, sessionId: String,
  deviceOs: String, audience: Option[Seq[String]]
) extends SAggregationEvent

class SAggEventNoAd(val eventType: EventType, val sessionId: String,
  val deviceOs: String,audience: Option[Seq[String]] = None) extends SAggregationEvent

case class EventJoin1(sessionId : String, key1 : String, value1 : Double)
case class EventJoin2(sessionId : String, key2 : String, value2 : Double)

