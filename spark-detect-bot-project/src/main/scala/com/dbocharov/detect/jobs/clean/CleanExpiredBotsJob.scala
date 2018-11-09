package com.dbocharov.detect.jobs.clean

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.dbocharov.detect.config.{CassandraConfig, CleanExpiredBotsJobConfig}

object CleanExpiredBotsJob {
  val keyspace = CassandraConfig.keyspace
  val table = CassandraConfig.table
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(CleanExpiredBotsJobConfig.APP_NAME)
      .setMaster("local[1]")
      .set("spark.cassandra.connection.keep_alive_ms", "600000")
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    while (true) {
     ss.sparkContext.cassandraTable(keyspace,table)
        .filter(row => row.getLong("block_date") + 1000 * CleanExpiredBotsJobConfig.expired_duration < System.currentTimeMillis())
        .deleteFromCassandra(CassandraConfig.keyspace, CassandraConfig.table)
      Thread.sleep(1000 * CleanExpiredBotsJobConfig.expired_duration)
    }

  }
}
