package ca.mcit.input.storeEnrichedStationInfo

import java.io.File
import java.net.URL
import org.apache.commons.io.FileUtils

object GetJSONFile {
  //Get JSON files
  def stationInformation(): Unit = {
    try {
      //Download json_data of station_information
      val fileURL = "https://api-core.bixi.com/gbfs/en/station_information.json"
      val fileName = "Feed/station_information.json"

      FileUtils.copyURLToFile(
        new URL(fileURL),
        new File(fileName))

      println(" Downloaded json_data of " + fileName + " to local")
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }

  def systemInformation(): Unit = {
    try {
      println("Download the Feed:")
      //Download json_data of station_information
      val fileURL = "https://api-core.bixi.com/gbfs/en/system_information.json"
      val fileName = "Feed/system_information.json"

      FileUtils.copyURLToFile(
        new URL(fileURL),
        new File(fileName))

      println(" Downloaded json_data of " + fileName + " to local")
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }
}








