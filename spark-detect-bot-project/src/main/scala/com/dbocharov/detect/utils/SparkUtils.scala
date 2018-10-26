package com.dbocharov.detect.utils

import com.datastax.spark.connector.cql.CassandraConnector
import com.dbocharov.detect.model.BotRecord
import org.apache.spark.sql.{Dataset, ForeachWriter, SparkSession}

object SparkUtils {

  def initSparkSession(app_name:String) ={
    SparkSession.builder()
      .appName(app_name)
      .master("local[*]")
      .config("spark.cassandra.connection.keep_alive_ms","600000")
      .getOrCreate()
  }

  implicit class BotWriter(stream:Dataset[BotRecord]){
  def writeBotToCassandra(cassandraConnector: CassandraConnector,keyspace: String, table: String) = {
    stream
      .writeStream
      .option("checkpointDit", "checkpoint_folder_str")
      .foreach(getCassandraWriter(cassandraConnector, keyspace, table))
      .outputMode("update")
      .start()
      .awaitTermination()
    }
  }
  def getCassandraWriter(cassandraConnector: CassandraConnector,keyspace:String,table:String) ={
    new ForeachWriter[BotRecord] {
      override def open(partitionId: Long, version: Long): Boolean = true
      override def process(value: BotRecord): Unit = {
        cassandraConnector.withSessionDo(session => session
          .execute(CassandraStatements
            .getInsertBotCqlQuery(
              keyspace,
              table,
              value.ip,
              value.block_date
            )))
      }
      override def close(errorOrNull: Throwable): Unit = {}
    }
  }
}
