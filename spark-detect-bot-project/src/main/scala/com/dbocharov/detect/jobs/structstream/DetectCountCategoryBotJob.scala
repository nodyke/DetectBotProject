package com.dbocharov.detect.jobs.structstream

import com.datastax.spark.connector.cql.CassandraConnector
import com.dbocharov.detect.config.{CassandraConfig, DetectBotConfig}
import com.dbocharov.detect.kafka.KafkaReader
import com.dbocharov.detect.model.BotRecord
import com.dbocharov.detect.utils.SparkUtils
import org.apache.spark.sql.functions._


object DetectCountCategoryBotJob {


  def main(args: Array[String]): Unit = {
    val bootstrap_server = if(args.length > 0) args(0) else "127.0.0.1:9092"
    val topic = if(args.length > 1) args(1) else "events"

    val sc = SparkUtils.initSparkSession(this.getClass.getName)

    val connector = CassandraConnector.apply(sc.sparkContext)


    import sc.implicits._
    import SparkUtils.BotWriter
    KafkaReader.getKafkaStructureStream(sc,bootstrap_server,topic)
    .select($"ip",$"category_id", window($"timestamp","10 minutes","30 seconds"))
    .distinct()
    .groupBy($"ip",$"window",$"category_id")
    .count()
    .where($"count">DetectBotConfig.count_category)
    .select($"ip")
    .withColumn("block_date",current_timestamp())
    .as[BotRecord]
    .writeBotToCassandra(connector,CassandraConfig.keyspace,CassandraConfig.table.concat("_per_request"))

  }
}
