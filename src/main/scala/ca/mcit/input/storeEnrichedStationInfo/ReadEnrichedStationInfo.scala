package ca.mcit.input.storeEnrichedStationInfo

import org.apache.spark.sql.{DataFrame, SparkSession}

//Spark SQL: use Spark SQL to read the enriched station information from HDFS in the form of CSV.
object ReadEnrichedStationInfo extends App{
  def readEnrichedStationInfo(): DataFrame = {
    val fileName = "hdfs://quickstart.cloudera/user/fall2019/minhle/final_project/feed_data/" +
      "enriched_station_system_information/enriched_sys_sta_info.csv"
    val spark = SparkSession.builder ().appName ("Spark SQL practice").master ("local[*]").getOrCreate ()

    val enrichedStationInfoDF: DataFrame = spark.read.option ("header", "false").option ("inferschema", "true").csv (fileName)

    //spark.stop ()
    enrichedStationInfoDF
  }
}
