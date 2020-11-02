package ca.mcit

import ca.mcit.input.storeEnrichedStationInfo._
import ca.mcit.input.trip._

object Main extends App{
  try {
    //Sprint 2 artifact, get Json info,enriched then put the enriched file to HDFS
    GetJSONFile.systemInformation()
    GetJSONFile.stationInformation()
    ConvertJSONtoCSV.convertJSONtoCSV()
    EnrichData.enrichStationsSystemInfo()
    DirectoriesManagement.createDirectories()
    FileManagement.uploadFiles()

    //Sprint 3 get Trips, subscribe trips and enriched it
    GetTrip.getTrips
    TripProducer.subscribeTrip()
    SparkStreaming.sparkStreaming()
  }
  catch {
    case _: Exception => println("Connection error! Please try again")
  }
}
