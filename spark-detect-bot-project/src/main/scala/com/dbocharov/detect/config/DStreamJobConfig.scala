package com.dbocharov.detect.config

import org.apache.spark.streaming.Seconds

object DStreamJobConfig {
  val APP_NAME = "DetectBotDStreamApp"
  val batch_duration = Seconds(30)
  val block_seconds = 600
}
