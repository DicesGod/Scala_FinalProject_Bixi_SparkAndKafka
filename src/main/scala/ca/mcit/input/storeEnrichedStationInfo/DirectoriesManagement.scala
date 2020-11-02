package ca.mcit.input.storeEnrichedStationInfo

import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.Path

object DirectoriesManagement {

  //create Directory method
  def createDirectory(filePath: Path,directory_name: String): Unit = {
    try {
      if (HadoopConnection.fs.exists(filePath)) {
        println(" "+ directory_name+ " directory already exists")
      }
      else {
        println(" "+ directory_name+ " directory is created")
        HadoopConnection.fs.mkdirs(filePath)
      }
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }

  //Manage directories on HDFS
  def createDirectories(): Unit = {
    println("DATA PIPELINE INSTALLATION: ")
    var filePath = new Path("/user/fall2019/minhle/final_project/feed_data")
    createDirectory(filePath,"feed_data")

    filePath = new Path("/user/fall2019/minhle/final_project/feed_data/enriched_station_system_information")
    createDirectory(filePath,"enriched_station_system_information")
  }
}


