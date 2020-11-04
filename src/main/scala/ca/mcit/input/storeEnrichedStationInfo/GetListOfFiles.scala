package ca.mcit.input.storeEnrichedStationInfo

import java.io.File

object GetListOfFiles {
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }
}
