package ca.mcit.input.trip

import java.io.{BufferedWriter, File, FileWriter}
import java.net.URL
import better.files._
import org.apache.commons.io.FileUtils
import scala.io.Source

object GetTrip {
  def getTrips: String = {
    try {
      //Download the file from stm.info
      FileUtils.copyURLToFile(new URL("https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip"),
        new File("/Users/minhle/Downloads/bixi.zip"))
      println(" Downloaded bixi.zip to local")

      //Unzip the file
      val zipFile: better.files.File = file"/Users/minhle/Downloads/bixi.zip"
      zipFile.unzipTo(destination = file"/Users/minhle/Downloads/bixi")
      println(" Unzipped bixi.zip")

      //Get 100 trips
      val myLine = {
        val src = Source.fromFile("/Users/minhle/Downloads/bixi/OD_2019-10.csv")
        val line = src.getLines.slice(1, 101).toList.mkString("\n")
        src.close
        line
      }
      println(" Created 100_trips.csv")
      val bw = new BufferedWriter(new FileWriter("/Users/minhle/Desktop/Projects/Scala_KAFKA_FinalProject_Bixi_Sprint3/Feed/100_trips.csv"))
      bw.write(myLine)
      bw.close()
      myLine
    }
    catch {
      case _: Exception => println("Connection error!")
        null
    }
  }
}


