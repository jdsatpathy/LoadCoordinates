package com.app
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.validation.GenericValidation
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LoadCoordinates extends LazyLogging with GenericValidation {
  def main(args: Array[String]): Unit = {
    val envProp = ConfigFactory.load().getConfig(args(0))
    val ssc = structuredStreamContext(envProp)
    writeKafkaDataToHdfs(envProp, kafkaDataStream(envProp, ssc))
    ssc.start()
    ssc.awaitTermination()
  }

  def sparkSessionCreator(envProp : Config, env : String): SparkSession = env match {
    case "dev" => this.sparkSessionWithoutHiveImplementation(envProp)
    case "prod" => this.sparkSessionWithHiveImplementation(envProp)
    case _ => this.sparkSessionWithoutHiveImplementation(envProp)
  }

  def sparkSessionWithHiveImplementation(envProp: Config): SparkSession = {
    SparkSession.builder().master(envProp.getString("spark.master"))
      .appName(envProp.getString("app.name"))
      .config("hive.metastore.uris", envProp.getString("hive.metastore.uris"))
      .config("spark.sql.warehouse.dir", envProp.getString("spark.sql.warehouse.dir"))
      .enableHiveSupport()
      .getOrCreate()
  }

  def sparkSessionWithoutHiveImplementation (envProp : Config) : SparkSession = {
    SparkSession.builder().config(this.sparkConf(envProp))
      .getOrCreate()
  }

  def sparkConf(envProp : Config) : SparkConf = {
    new SparkConf()
      .setMaster(envProp.getString("spark.master"))
      .setAppName(envProp.getString("app.name"))
  }

  def structuredStreamContext(envProp : Config) : StreamingContext = {
    new StreamingContext(sparkConf(envProp),Seconds(30))
  }

  def kafkaDataStream(envProp : Config, ssc : StreamingContext) : DStream[String]  = {
    val topics = Array(envProp.getString("topic.name"))
    val kafkaParam = Map[String, Object] (
      "bootstrap.servers" -> envProp.getString("bootstrap.server"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false : java.lang.Boolean))
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParam))
    stream.map(r => r.value())
  }

  def writeKafkaDataToHdfs(envProp : Config, df : DStream[String]) : Unit = {
    df.saveAsTextFiles(envProp.getString("output.dir"))

  }

//  def writeToHive(envProp : Config , df : DStream[String]) : Unit = {
//    df.
//
//  }

  def writeToMysql(df : InputDStream[ConsumerRecord[String, String]], envProp : Config) : Unit = {
    val prop = new Properties()
    prop.put("driver", envProp.getString("mysql.driver"))
    prop.put("user", envProp.getString("mysql.user"))
    prop.put("password", envProp.getString("mysql.password"))
//    df.writeStream.outputMode("APPEND").format("jdbc").trigger(Trigger.Continuous("1 second"))
//    df.write.mode("append")
//      .jdbc(envProp.getString("mysql.connection"),envProp.getString("mysql.table"),prop)

  }
}