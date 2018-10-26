package com.dbocharov.detect.jobs.structstream

import com.dbocharov.detect.kafka.KafkaReader
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions._
import com.dbocharov.detect.model._
import com.dbocharov.detect.utils.SparkUtils
import com.dbocharov.detect.config.{CassandraConfig, DetectBotConfig}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object DetectPerRequestBotJob {

  def detect(sc:SparkSession,dataset:Dataset[Row],per_request:Int)={
    import sc.implicits._
    dataset
      .groupBy($"ip",window($"timestamp","10 minutes"))
      .count()
      .where($"count" > per_request)
      .select($"ip")
      .withColumn("block_date",current_timestamp())
      .as[BotRecord]
  }


  def main(args: Array[String]): Unit = {
    val bootstrap_server = if(args.length > 0) args(0) else "127.0.0.1:9092"
    val topic = if(args.length > 1) args(1) else "events"

    val sc = SparkUtils.initSparkSession(this.getClass.getName)

    val connector = CassandraConnector.apply(sc.sparkContext)

    import SparkUtils.BotWriter
    detect(sc,KafkaReader.getKafkaStructureStream(sc,bootstrap_server,topic),DetectBotConfig.per_req)
      .writeBotToCassandra(connector,CassandraConfig.keyspace,CassandraConfig.table.concat("_per_request"))

  }

}
