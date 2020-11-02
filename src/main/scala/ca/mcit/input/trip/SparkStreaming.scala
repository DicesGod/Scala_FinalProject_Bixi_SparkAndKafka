package ca.mcit.input.trip

import ca.mcit.model.Trip
import ca.mcit.input.storeEnrichedStationInfo.ReadEnrichedStationInfo
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Spark Streaming: stream Kafka topic of trip into Spark DStream using Spark Streaming component
object SparkStreaming extends App {
  def sparkStreaming(): Unit = {
    // 1. Create Spark streaming context
    val spark = SparkSession.builder()
      .master("local[*]").appName("Spark streaming with Kafka for join")
      .getOrCreate()
    // 1.c A Spark streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // 2. Create DStream
    // 2.a Create Kafka consuming configuration
    val kafkaConfig = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    // 2.b Create the stream and subscribe to the topic(s)
    val topic = "fall2019_minhle_trip"
    val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
    )

    // 3. Business logic for each micro-batch (a micro-batch is an RDD)
    // for each micro-batch, join the trip with enriched station info
    inStream.map(_.value()).foreachRDD(microBatchRdd => joinTripandEnrichedStationInfo(microBatchRdd))

    // 4. Start streaming and keep running
    ssc.start()
    ssc.awaitTermination()

      def joinTripandEnrichedStationInfo(rdd: RDD[String]): Unit = {
        import spark.implicits._
        val tripRdd: RDD[Trip] = rdd.map(csvRating => Trip(csvRating))
        val tripDF = tripRdd.toDF()

        val enrichedStationInfoDF = ReadEnrichedStationInfo.readEnrichedStationInfo()

       tripDF.createOrReplaceTempView("trip")
        enrichedStationInfoDF.createOrReplaceTempView("enrichedStaInfo")

        val enriched_info = spark.sql (
          """SELECT `start_date`,`start_station_code`,
            |`end_date`,`end_station_code`,
            |`duration_sec`,`is_member`,
            |_c0 as system_id,_c1 as timezone, _c2 as station_id, _c3 as name,
            |_c4 as short_name, _c5 as lat, _c6 as lon, _c7 as capacity
            |FROM trip t JOIN enrichedStaInfo e
            |where t.start_station_code = e._c4
            |"""
            .stripMargin)

        enriched_info.coalesce (1).write.mode (SaveMode.Append).csv ("Result/")
      }
  }
  sparkStreaming()
}
