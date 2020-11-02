package ca.mcit.model

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HadoopConnection{
  val conf = new Configuration()
  conf.addResource(new Path("/Users/minhle/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/minhle/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)
}
