package com.dbocharov.detect.jobs.dstream

import com.datastax.spark.connector.streaming._
import com.dbocharov.detect.config.{CassandraConfig, DStreamJobConfig, DetectBotConfig, KafkaConfig}
import com.dbocharov.detect.model.{BotRecord, Event}
import com.dbocharov.detect.utils.DetectBotUtils
import com.google.gson.{GsonBuilder, JsonObject, JsonParser}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object DetectBotJob {

  private val logger = Logger.getLogger(getClass)


  def getKafkaParams(server:String):Map[String,Object] = {
    //Temp for testing, need for generate group id, cause temporary can't reset consumer group offset, some bug
    Map[String, Object](
      "bootstrap.servers" -> server,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> DStreamJobConfig.APP_NAME.concat("_group"),
      "auto.offset.reset" -> KafkaConfig.auto_offset_reset_policy ,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

    private val jsonParser = new JsonParser()
    private val gson = new GsonBuilder().setLenient().create()
    private def mapJsonEvent(jsonObject: JsonObject):Event = gson.fromJson(jsonObject,classOf[Event])
    def mapToEvent(stream: DStream[String]) ={
      stream
        .map(json => jsonParser.parse(json))
        .map(el => el.getAsJsonObject)
        .map(jsonObject => mapJsonEvent(jsonObject))
    }


  def getEventsFromKafka(server:String,topics:Array[String],scc:StreamingContext):DStream[Event] = {
    mapToEvent(KafkaUtils.createDirectStream[String, String](
      scc,
      PreferConsistent,
      Subscribe[String, String](topics, getKafkaParams(server))
    )
      .map(record => record.value().trim)
      .filter(json => !json.isEmpty)
    )
      .filter(event => DetectBotConfig.types.contains(event.event))
      .persist()
  }

  def detectHighDifferenceEventsBot(stream:DStream[Event],max_seconds:Int):DStream[BotRecord] = {
    stream
      .map(event => (event.ip,event))
      .groupByKey()
      .mapValues(iter => DetectBotUtils.checkHighDifferenceBot(iter,max_seconds))
      .filter(pair => pair._2)
      .map(pair => BotRecord(pair._1,System.currentTimeMillis()))
  }

  def detectPerRequestBot(stream:DStream[Event],max_count:Long):DStream[BotRecord] = {
    stream
      .map(event => (event.ip,1))
      .reduceByKeyAndWindow((accum, sum) => accum + sum, Seconds(600))
      .filter(pair => pair._2 > max_count)
      .map(pair => BotRecord(pair._1,System.currentTimeMillis()))
  }

  def detectCountCategoryBot(stream: DStream[Event],max_count:Long):DStream[(BotRecord,Int)] = {
    stream
      .map(event => (event.ip,event.category_id))
      .transform(rdd => rdd.distinct())
      .groupByKeyAndWindow(Seconds(600))
      .map(pair => (pair._1, pair._2.size))
      .filter(pair => pair._2 > max_count)
      .map(pair => (BotRecord(pair._1, System.currentTimeMillis()),pair._2))
  }

  def initSparkStreamingContext(checkpoint_dir:String) = {
    val sparkConf = new SparkConf()
      .setAppName(DStreamJobConfig.APP_NAME)
      .setMaster("local[*]")
      .set("spark.cassandra.connection.keep_alive_ms","600000")
    val scc = new StreamingContext(sparkConf, DStreamJobConfig.batch_duration)
    scc.checkpoint(checkpoint_dir)
    scc
  }

  def main(args: Array[String]): Unit = {
    val bootstrap_server = if(args.length > 0) args(0) else "127.0.0.1:9092"
    val topics = if(args.length > 1) Array(args(1)) else Array("events")

    //Init spark app and spark context
    val scc = initSparkStreamingContext("checkpoint_dir")

    //Get event from kafka
    val stream = getEventsFromKafka(bootstrap_server,topics,scc)

    //calc per request and detect bots, slide interval = batch interval, default 30 seconds
    val stream_per_r = detectPerRequestBot(stream,DetectBotConfig.per_req)
    stream_per_r.print()
    stream_per_r.saveToCassandra(CassandraConfig.keyspace,CassandraConfig.table.concat("_per_request"))

    //count categories in each ip, slide interval = batch duration, default = 30 seconds
    val stream_count_categories = detectCountCategoryBot(stream,DetectBotConfig.count_category)
    stream_count_categories.print()
    stream_count_categories
      .map(pair => pair._1)
      .saveToCassandra(CassandraConfig.keyspace,CassandraConfig.table.concat("_count_categories"))

    //detect high difference events
    val stream_high_diff  = detectHighDifferenceEventsBot(stream,DetectBotConfig.max_diff)
    stream_high_diff.print()
    stream_high_diff.saveToCassandra(CassandraConfig.keyspace,CassandraConfig.table.concat("_high_diff"))

    scc.start()
    scc.awaitTermination()
  }

}

