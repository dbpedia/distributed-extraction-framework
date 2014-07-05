package org.dbpedia.extraction.util

import org.apache.spark.{SparkContext, SparkConf}
import org.dbpedia.extraction.dump.extract.DistConfig
import org.apache.log4j.{Logger, Level}
import java.nio.file.{Paths, Files}
import java.io._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.spark.serialize.KryoSerializationWrapper
import org.apache.hadoop.conf.Configuration

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
   * Turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
   * former values
   */
  def silenceSpark() =
  {
    setLogLevels(Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka"))
  }

  /**
   * Creates and returns a new SparkContext taking configuration info from Config
   * @param config
   * @return
   */
  def getSparkContext(config: DistConfig) =
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
      //conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", "org.dbpedia.extraction.spark.serialize.KryoExtractionRegistrator")
      conf.set("spark.kryoserializer.buffer.mb", "100")
      sc = new SparkContext(conf)
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

  /**
   * Serialize an object to string and store it into Hadoop's Configuration object.
   *
   * @param key String key to store against in conf
   * @param obj Object to serialize
   * @param conf Configuration
   */
  def storeObjectToConfiguration(key: String, obj: AnyRef, conf: Configuration)
  {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    os.writeObject(obj)
    os.close()
    conf.set(key, bos.toString("UTF-8"))
  }

  /**
   * Deserialize an object stored as a serialized string in Hadoop's Configuration object.
   *
   * @param key String key to retrieve from
   * @param conf Configuration
   * @return deserialized object
   */
  def getObjectFromConfiguration(key: String, conf: Configuration): AnyRef =
  {
    val serialized = conf.get(key)
    new ObjectInputStream(new ByteArrayInputStream(serialized.getBytes("UTF-8"))).readObject()
  }
}
