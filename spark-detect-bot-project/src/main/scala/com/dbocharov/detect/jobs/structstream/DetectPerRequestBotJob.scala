package com.dbocharov.detect.jobs.structstream

import com.dbocharov.detect.kafka.KafkaReader
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.dbocharov.detect.model._
import com.dbocharov.detect.utils.{CassandraStatements, SparkUtils}
import org.apache.spark.sql.types.StructType
import com.dbocharov.detect.config.{CassandraConfig, DetectBotConfig}


object DetectPerRequestBotJob {


  private val bot_schema = ScalaReflection.schemaFor[BotRecord].dataType.asInstanceOf[StructType]

  def main(args: Array[String]): Unit = {
    val bootstrap_server = if(args.length > 0) args(0) else "127.0.0.1:9092"
    val topic = if(args.length > 1) args(1) else "events"

    val sc = SparkUtils.initSparkSession(this.getClass.getName)

    val connector = CassandraConnector.apply(sc.sparkContext)

    import sc.implicits._
    import SparkUtils.BotWriter
    KafkaReader.getKafkaStructureStream(sc,bootstrap_server,topic)
      .groupBy(window($"timestamp","10 minutes","30 seconds"),$"ip")
      .count()
      .where($"count" > DetectBotConfig.per_req)
      .select($"ip")
      .withColumn("block_date",current_timestamp())
      .as[BotRecord]
      .writeBotToCassandra(connector,CassandraConfig.keyspace,CassandraConfig.table.concat("_per_request"))

  }

}
