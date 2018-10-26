package com.dbocharov.tests

import com.dbocharov.detect.jobs.dstream.DetectBotJob
import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{FunSuite}

class DetectBotDStreamTest extends FunSuite with StreamingSuiteBase{
 private val fileName:String = "test.json"


  test("DStream Testing per request in 10 minutes count") {

    val input = List(sc.textFile(fileName).collect().toList)

    def testPerReqCount(stream: DStream[String]) = {
      DetectBotJob.detectPerRequestBot(DetectBotJob.mapToEvent(stream),5)
        .count()

    }

    val expected = List(List(1L))

    testOperation(input, testPerReqCount _, expected, ordered = false)

  }

  test("DStream testing count category in 10 minutes"){

    val input = List(sc.textFile(fileName).collect().toList)

    def testCountCategory(stream:DStream[String]) ={
      DetectBotJob.detectCountCategoryBot(DetectBotJob.mapToEvent(stream),5)
        .count()
    }

    val expected = List(List(1L))

    testOperation(input, testCountCategory _, expected, ordered = false)
  }

  test("DStream testing high difference"){
    val input = List(sc.textFile(fileName).collect().toList)

    def testHighDifference(stream:DStream[String]) ={
      DetectBotJob.detectHighDifferenceEventsBot(DetectBotJob.mapToEvent(stream),50)
        .count()
    }

    val expected = List(List(1L))

    testOperation(input,testHighDifference _,expected,ordered = false)
  }
}
