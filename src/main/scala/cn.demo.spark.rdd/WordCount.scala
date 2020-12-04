package cn.demo.spark.rdd

import com.alibaba.fastjson.JSON
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

    val ssc = new StreamingContext(config,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.92.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("data_transfer")

    val kafkaDirectStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val platName = kafkaDirectStream.map(_.value).foreachRDD(rdd => {
      rdd.foreach(record => {
        val json = JSON.parseObject(record)
        val platformName = json.get("plat_name")
        System.out.println("platName:"+platformName)
      })
    })


    //开始计算
    ssc.start()
    ssc.awaitTermination()
    //stream.foreach(println)

  }
}
