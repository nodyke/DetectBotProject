package com.dbocharov.detect.jobs.clean

import com.dbocharov.detect.model.BotRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.dbocharov.detect.config.{CassandraConfig, CleanExpiredBotsJobConfig}

object CleanExpiredBotsJob {
  val sql = "select * from test01.bot"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(CleanExpiredBotsJobConfig.APP_NAME)
      .setMaster("local[1]")
      .set("spark.cassandra.connection.keep_alive_ms","600000")
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    while (true) {
      ss.sql(sql).rdd
        .map(row => BotRecord(row.getString(0), row.getLong(1)))
        .filter(BotRecord => BotRecord.block_date + 1000 * CleanExpiredBotsJobConfig.expired_duration < System.currentTimeMillis())
        .deleteFromCassandra(CassandraConfig.keyspace, CassandraConfig.table)
      Thread.sleep(1000*CleanExpiredBotsJobConfig.expired_duration)
    }

  }
}
