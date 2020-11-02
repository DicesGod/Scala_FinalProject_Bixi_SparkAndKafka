package ca.mcit.input.storeEnrichedStationInfo

import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.Path

object FileManagement {
  //uploadFile to HDFS
  def uploadFiles(): Unit = {
    try {
      println("File Management:")
      val filePath = new Path("/user/fall2019/minhle/final_project/feed_data/enriched_station_system_information/enriched_sta_sys_info.csv")
      val srcPath = new Path("/Users/minhle/Desktop/Projects/Scala_KAFKA_FinalProject_Bixi_Sprint3/Feed/enriched_sta_sys_info/enriched_sys_sta_info.csv")
      val dstPath = new Path("/user/fall2019/minhle/final_project/feed_data/enriched_station_system_information")
      uploadFile("enriched_sta_sys_info", filePath, srcPath, dstPath)
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }

  def uploadFile(fileName: String,filePath: Path,srcPath: Path,dstPath: Path): Unit = {
    if (HadoopConnection.fs.exists (filePath))  {
    HadoopConnection.fs.delete (filePath, true)
    HadoopConnection.fs.copyFromLocalFile (srcPath,dstPath)
    println (" Deleted and uploaded the new "+ fileName+".csv file to HDFS")
    }
    else {
    HadoopConnection.fs.copyFromLocalFile (srcPath,dstPath)
    println (" Uploaded the new "+ fileName+".csv file to HDFS")
    }
  }
}


