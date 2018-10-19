package com.dbocharov.jobs.clean

import com.dbocharov.jobs.model.BotRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object CleanExpiredBotsJob {

  val sql = "select * from test01.bot_count_cat"


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
        .deleteFromCassandra("test01", "bot_count_cat")
      Thread.sleep(1000*CleanExpiredBotsJobConfig.expired_duration)
    }

  }
}
