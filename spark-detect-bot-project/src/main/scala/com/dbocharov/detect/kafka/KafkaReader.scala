package com.dbocharov.detect.kafka

import com.dbocharov.detect.config.{DetectBotConfig, KafkaConfig}
import com.dbocharov.detect.model.Event
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType

object KafkaReader {
  private val event_schema = ScalaReflection.schemaFor[Event].dataType.asInstanceOf[StructType]

  def getKafkaStructureStream(sc: SparkSession, server: String, topic: String) = {
    import sc.implicits._
    sc.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic)
      .option("startingOffsets", KafkaConfig.auto_offset_reset_policy)
      .load
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp as TIMESTAMP)")
      .select(from_json($"value", event_schema) as "event", $"timestamp")
      .select("event.*", "timestamp")
      .where($"event" isin (DetectBotConfig.types: _*))
  }

}
