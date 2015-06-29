package org.dbpedia.extraction.util

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.dbpedia.extraction.dump.extract.DistConfig
import org.apache.log4j.{Logger, Level}
import java.nio.file.{Paths, Files}
import java.io.FileNotFoundException
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.spark.serialize.KryoSerializationWrapper
import org.apache.spark.ui.jobs.DBpediaJobProgressListener

/**
 * Utility functions specific to Spark
 */
object SparkUtils
{
  /**
   * Stores the SparkContext instance.
   */
  private var sc: SparkContext = null

  /**
   * Set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) =
  {
    loggers.map
    {
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

  /**
   * Sets log levels for Spark and its peripheral libraries to DistConfig.sparkLogLevel.
   */
  def setSparkLogLevels(config: DistConfig) =
  {
    setLogLevels(config.sparkLogLevel, Seq("org.apache", "spark", "org.eclipse.jetty", "akka"))
  }

  /**
   * Creates and returns a new SparkContext taking configuration info from Config
   * @param config
   * @return
   */
  def getSparkContext(config: DistConfig) =
  synchronized
  {
    if (sc == null)
    {
      val conf = new SparkConf().setMaster(config.sparkMaster).setAppName(config.sparkAppName)
      for ((property, value) <- config.sparkProperties)
        conf.set(property, value)
      conf.setSparkHome(config.sparkHome)
      val distJarName = if (Files.exists(Paths.get("target/distributed-4.0-SNAPSHOT.jar")))
      {
        "target/distributed-4.0-SNAPSHOT.jar"
      } else if (Files.exists(Paths.get("distributed/target/distributed-4.0-SNAPSHOT.jar")))
      {
        "distributed/target/distributed-4.0-SNAPSHOT.jar"
      } else
      {
        throw new FileNotFoundException("distributed-4.0-SNAPSHOT.jar cannot be found in distributed/target. Please run mvn install -Dmaven.test.skip=true to build JAR first.")
      }

      conf.setJars(List(distJarName))
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", "org.dbpedia.extraction.spark.serialize.KryoExtractionRegistrator")
      conf.set("spark.kryoserializer.buffer.mb", "100")
      sc = new SparkContext(conf)
      // No logging is done upon omitting 'with Logging' - some package problem?
      setLogLevels(Level.INFO, Seq("org.apache.spark.ui.jobs.DBpediaJobProgressListener"))
      sc.addSparkListener(new DBpediaJobProgressListener(conf))
    }
    sc
  }

  /**
   * Return an iterator that contains all of the elements in given RDD.
   * The iterator will consume as much memory as the largest partition in the RDD.
   *
   * @param rdd
   * @return iterator for rdd's elements
   */
  def rddToLocalIterator[T: ClassTag](rdd: RDD[T]): Iterator[T] =
  {
    def collectPartition(p: Int): Array[T] =
    {
      sc.runJob(rdd, (iter: Iterator[T]) => iter.toArray, Seq(p), allowLocal = false).head
    }
    (0 until rdd.partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Returns the function object wrapped inside a KryoSerializationWrapper.
   * This is useful for having Kryo-serialization for Spark closures.
   *
   * @param function
   * @return
   */
  def kryoWrapFunction[T, U](function: (T => U)): (T => U) =
  {
    def genMapper(kryoWrapper: KryoSerializationWrapper[(T => U)])(input: T): U =
    {
      kryoWrapper.value.apply(input)
    }

    genMapper(KryoSerializationWrapper(function)) _
  }
}
