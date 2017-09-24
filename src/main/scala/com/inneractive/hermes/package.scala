package com.inneractive

import com.inneractive.hermes.kafka.streams.HermesConfig

/**
  * Created by Richard Grossman on 2017/08/31.
  */
package object hermes {
  val config = HermesConfig
}

trait CliStarter {
  def parseCli(args : Array[String]) = {
    val parser = new scopt.OptionParser[HermesParams]("Hermes Sample") {
      head("hermes", "1.x")

      opt[Unit]('c', "cleanup").action((x, c) =>
        c.copy(cleanup = true)).text("Clean up kafka stream state")
    }

    val appConfig = parser.parse(args, HermesParams())

    appConfig
  }
}

case class HermesParams(cleanup: Boolean = false)

