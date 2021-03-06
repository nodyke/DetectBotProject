package com.dbocharov.detect.jobs.structstream

import com.datastax.spark.connector.cql.CassandraConnector
import com.dbocharov.detect.config.{CassandraConfig, DetectBotConfig}
import com.dbocharov.detect.kafka.KafkaReader
import com.dbocharov.detect.model.{BotRecord, Event}
import com.dbocharov.detect.utils.{DetectBotUtils, SparkUtils}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object DetectHighDifferenceBotJob {

  case class IsBotEntity(
                          ip: String,
                          isBot: Boolean
                        )

  def detect(sc: SparkSession, dataset: Dataset[Row], max_diff: Int) = {
    def mappingFunction(key: String, iterator: Iterator[Event], max_diff: Int) = {
      IsBotEntity(key, DetectBotUtils.checkHighDifferenceBot(iterator.toIterable, max_diff))
    }
    import sc.implicits._
    dataset
      .select($"ip", $"unix_time", $"category_id", $"event")
      .as[Event]
      .groupByKey(_.ip)
      .mapGroups { case (key, iterator) => mappingFunction(key, iterator, max_diff) }
      .filter(_.isBot)
      .select($"ip")
      .withColumn("block_date", current_timestamp())
      .as[BotRecord]
  }

  def main(args: Array[String]): Unit = {
    val bootstrap_server = if (args.length > 0) args(0) else "127.0.0.1:9092"
    val topic = if (args.length > 1) args(1) else "events"
    val sc = SparkUtils.initSparkSession(this.getClass.getName)
    val connector = CassandraConnector.apply(sc.sparkContext)
    import SparkUtils.BotWriter
    detect(sc, KafkaReader.getKafkaStructureStream(sc, bootstrap_server, topic), DetectBotConfig.max_diff)
      .writeBotToCassandra(connector, CassandraConfig.keyspace, CassandraConfig.table)
  }

}
