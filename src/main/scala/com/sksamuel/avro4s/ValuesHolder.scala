package com.sksamuel.avro4s

/**
  * Created by Richard Grossman on 2017/10/10.
  */
case class ValuesHolder(double: Array[Double], ints: Array[Int]) {
  def +(v: ValuesHolder): ValuesHolder =
    ValuesHolder(this.double.zip(v.double).map(v => v._1 + v._2), this.ints.zip(v.ints).map(v => v._1 + v._2))
}
