package cn.demo.spark.rdd

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object WordCount {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
//    val sc = new SparkContext(config)
//
//    //println(sc)
//
//    val lines: RDD[String] = sc.textFile("in/word.txt")
//
//    val words: RDD[String] = lines.flatMap(_.split(" "))
//
//    val wordToOne: RDD[(String,Int)] = words.map((_,1))
//
//    val wordToSum: RDD[(String,Int)] = wordToOne.reduceByKey(_+_)
//
//    val result: Array[(String,Int)] = wordToSum.collect()
//
//    result.foreach(println)

    val ssc = new StreamingContext(config,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.224.130:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("myTopic")

    val kafkaDirectStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    kafkaDirectStream.map(_.value).print()

    //开始计算
    ssc.start()
    ssc.awaitTermination()
    //stream.foreach(println)

  }
}
