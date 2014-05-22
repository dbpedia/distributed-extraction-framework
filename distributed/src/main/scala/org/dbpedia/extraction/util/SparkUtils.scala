package org.dbpedia.extraction.util

import org.apache.spark.{SparkContext, SparkConf}
import org.dbpedia.extraction.dump.extract.DistConfig
import org.apache.log4j.{Logger, Level}

/**
 * Created by nilesh on 22/5/14.
 */
object SparkUtils
{
  /**
   * Set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map{
                 loggerName =>
                   val logger = Logger.getLogger(loggerName)
                   val prevLevel = logger.getLevel()
                   logger.setLevel(level)
                   loggerName -> prevLevel
               }.toMap
  }

  /**
   * Turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
   * former values
   */
  def silenceSpark() = {
    setLogLevels(Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka"))
  }

  /**
   * Creates and returns a new SparkContext taking configuration info from Config
   * @param config
   * @return
   */
  def getSparkContext(config: DistConfig) = {
    val conf = new SparkConf().setMaster(config.sparkMaster).setAppName(config.sparkAppName)
    for ((property, value) <- config.sparkProperties)
      conf.set(property, value)
    conf.setSparkHome(config.sparkHome)
    conf.setJars(List("target/distributed-4.0-SNAPSHOT.jar"))
    //conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.dbpedia.extraction.spark.serialize.KryoExtractionRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "50")
    new SparkContext(conf)
  }
}
