package com.dbocharov.jobs.dstream

import org.apache.spark.streaming.Seconds

object DStreamJobConfig {
  val keyspace = "test01"
  val table = "bot"
  val types = Array("click","view")
  val APP_NAME = "DetectBotDStreamApp"
  val batch_duration = Seconds(30)
  val auto_offset_reset_policy = "earliest"
  val block_seconds = 600

}
