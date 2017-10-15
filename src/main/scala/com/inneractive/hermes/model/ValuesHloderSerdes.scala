package com.inneractive.hermes.model

import com.sksamuel.avro4s.AvroInputStream
import com.sksamuel.avro4s.AvroOutputStream
import com.sksamuel.avro4s.ValuesHolder
import java.io.ByteArrayOutputStream
import java.util
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import scala.util.Try

/**
  * Created by Richard Grossman on 2017/10/10.
  */
class ValueHolderSerializer extends Serializer[ValuesHolder] {

  override def configure(map: util.Map[String, _], b: Boolean) = {}

  override def serialize(topic: String, value: ValuesHolder) = {
    Option(value) map { v =>
      val baos = new ByteArrayOutputStream()
      val output = Try(AvroOutputStream.data[ValuesHolder](baos))
      val write = output.map(_.write(v))
      val flush = output.map(_.flush)
      val close = output.map(_.close())
      write.flatMap(_ => flush).flatMap(_ => close)
      baos.toByteArray
    } getOrElse Array.empty[Byte]
  }

  override def close() = {}
}

class ValueHolderDeserializer extends Deserializer[ValuesHolder] {
  override def configure(map: util.Map[String, _], b: Boolean) = {}

  override def close() = {}

  override def deserialize(s: String, bytes: Array[Byte]) = {
    val read = Try(AvroInputStream.binary[ValuesHolder](bytes))
    val obj = read.map {
      _.iterator().toSeq match {
        case Nil => null
        case other => other.head
      }
    }.getOrElse(null)
    val close = read.map(_.close())
    read.flatMap(_ => close)
    obj
  }
}


