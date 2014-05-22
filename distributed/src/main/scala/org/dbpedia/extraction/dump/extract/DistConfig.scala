package org.dbpedia.extraction.dump.extract

import java.net.InetAddress
import java.util.Properties
import scala.collection.JavaConversions.asScalaSet

/**
 * Class for distributed configuration
 */
class DistConfig(config: Properties)
{
  // It is recommended that spark-home and spark-master are explicitly provided.
  val sparkHome = config.getProperty("spark-home", sys.env.get("SPARK_HOME").getOrElse(""))

  // By default assume master is runnning locally; use 4 cores
  val sparkMaster = config.getProperty("spark-master", "local[4]")

  // Number of splits the initial RDD will be broken to - configure according to your cluster. Maybe total number of cores?
  val sparkNumSlices = config.getProperty("spark-num-slices", "4").toInt

  // Shows up on Spark Web UI
  val sparkAppName = config.getProperty("spark-appname", "dbpedia-distributed-extraction-framework")

  // Map of optional spark configuration properties.
  // See http://spark.apache.org/docs/latest/configuration.html
  val sparkProperties = config.stringPropertyNames().filter(_.startsWith("spark")).map(x => (x, config.getProperty(x))).toMap
}
