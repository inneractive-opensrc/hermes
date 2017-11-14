package com.inneractive.hermes.kafka.services

import io.finch._
import org.apache.kafka.streams.KafkaStreams


/**
  * Created by Richard Grossman on 2017/11/13.
  */
class CreativeIdService(stream : KafkaStreams, port : Int) {
//  var server : ListeningServer = _
//  val stateStore = Option(stream.store("cardinalityStore", QueryableStoreTypes.keyValueStore[String,Double]()))
//
//  case class CreativeResult(creativeId : String, revenue : Double)
//  object CreativeResult {
//    implicit val decoder: Decoder[CreativeResult] = deriveDecoder[CreativeResult]
//    implicit val encoder: Encoder[CreativeResult] = deriveEncoder[CreativeResult]
//  }

  def test: Endpoint[String] = get("creativeId" :: path[String]) { creativeId :String =>
    Ok(s"Test ${creativeId}")
  }


  //  import CreativeResult._
//  def getCreativeTotal: Endpoint[String] = get(path[String])
//   { creativeId : String => Ok("test")
//    stateStore.map { s =>
//      val value = s.get(creativeId)
//      Ok(CreativeResult(creativeId, value))
//    } getOrElse(throw new Exception("Failed to access StateStore"))
//  }
//  handle {
//    case e : Exception => BadRequest(e)
//  }

//  val getAllCreative = get("creatives") {
//    var creativeSeq = Seq.empty[CreativeResult]
//    stateStore.map {s =>
//      val iterator = s.all()
//      while(iterator.hasNext) {
//        val value = iterator.next()
//        creativeSeq = creativeSeq :+ CreativeResult(value.key,value.value)
//      }
//      Ok(creativeSeq)
//    } getOrElse(throw new Exception("Failed to access StateStore"))
//  } handle {
//    case e : Exception => BadRequest(e)
//  }

  def start() = ???
//  {
//    server = Http.server.serve(":7878", getCreativeTotal.toServiceAs[Application.Json])
//    Await.ready(server)
//  }

  def stop() = ???
    //server.close(Duration(5000, TimeUnit.MILLISECONDS))

}
