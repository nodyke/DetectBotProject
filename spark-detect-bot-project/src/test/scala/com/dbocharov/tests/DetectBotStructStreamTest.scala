package com.dbocharov.tests

import com.dbocharov.detect.jobs.structstream._
import com.dbocharov.detect.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.functions._

class DetectBotStructStreamTest extends FunSuite with BeforeAndAfterAll {
  private var spark_session:SparkSession = _
  private val fileName:String = "test.json"
  private var ds:Dataset[Row] = _

  test("Detect per request bot in struct stream"){
    assert(DetectPerRequestBotJob.detect(spark_session,ds,5).count() == 1)
  }

  test("Detect count category in struct stream"){
    assert(DetectCountCategoryBotJob.detect(spark_session,ds,5).count() == 1)
  }

  test("Detect high difference bot in struct stream"){
    assert(DetectHighDifferenceBotJob.detect(spark_session,ds,50).count() == 1)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark_session = SparkUtils.initSparkSession(this.getClass.getName)
    ds = spark_session.read.json(fileName)
      .withColumn("timestamp",current_timestamp())
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }


}
