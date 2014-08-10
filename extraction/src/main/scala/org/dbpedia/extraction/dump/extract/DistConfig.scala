package org.dbpedia.extraction.dump.extract

import java.util.Properties
import scala.collection.JavaConversions.asScalaSet
import org.dbpedia.extraction.util.ConfigUtils.getValue
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.net.URI
import org.apache.log4j.Level
import org.dbpedia.extraction.util.HadoopConfigurable
import org.apache.hadoop.fs.Path

/**
 * Class for distributed configuration. Delegates general stuff except directory/file properties to Config.
 *
 * Note that dumpDir, ontologyFile and mappingsDir are first checked in distConfigProps;
 * if not found they're checked in extractionConfigProps.
 *
 * @param distConfigProps Distributed extraction configuration properties
 * @param extractionConfigProps General extraction framework configuration properties
 * @see Config
 */
class DistConfig(distConfigProps: Properties, extractionConfigProps: Properties, val extractionConfigFile: URI) extends HadoopConfigurable
{
  private val extractionConfig = new ExtractionConfig()

  /** It is recommended that spark-home and spark-master are explicitly provided. */
  val sparkHome = distConfigProps.getProperty("spark-home", sys.env.get("SPARK_HOME").getOrElse(""))

  /** By default assume master is runnning locally; use 4 cores */
  val sparkMaster = distConfigProps.getProperty("spark-master", "local[4]")

  /** Shows up on Spark Web UI */
  val sparkAppName = distConfigProps.getProperty("spark-appname", "dbpedia-distributed-extraction-framework")

  /**
   * The StorageLevel to be used when calling RDD.persist() unless otherwise specified. Choose any of these:
   * MEMORY_ONLY
   * MEMORY_AND_DISK
   * MEMORY_ONLY_SER
   * MEMORY_AND_DISK_SER
   * DISK_ONLY
   * MEMORY_ONLY_2, MEMORY_AND_DISK_2 etc.
   *
   * By default it is set to MEMORY_AND_DISK_SER
   *
   * @see org.apache.spark.storage.StorageLevel
   */
  val sparkStorageLevel = Option(
                                  getValue(distConfigProps, "spark-storage-level", required = false)
                                  {
                                    level => StorageLevel.getClass.getDeclaredMethod(level).invoke(StorageLevel).asInstanceOf[StorageLevel]
                                  }
                                ).getOrElse(StorageLevel.MEMORY_AND_DISK_SER)

  /** Map of optional spark configuration properties. See http://spark.apache.org/docs/latest/configuration.html */
  val sparkProperties = distConfigProps.stringPropertyNames().filter(_.startsWith("spark.")).map(x => (x, distConfigProps.getProperty(x))).toMap

  /** Path to hadoop core-site.xml */
  override protected val hadoopCoreConf = distConfigProps.getProperty("hadoop-coresite-xml-path")

  /** Path to hadoop hdfs-site.xml */
  override protected val hadoopHdfsConf = distConfigProps.getProperty("hadoop-hdfssite-xml-path")

  /** Path to hadoop mapred-site.xml */
  override protected val hadoopMapredConf = distConfigProps.getProperty("hadoop-mapredsite-xml-path")

  /** This is used for setting log levels for "org.apache", "spark", "org.eclipse.jetty" and "akka" using
    * SparkUtils.setLogLevels(). It is WARN by default.
    */
  val sparkLogLevel = Level.toLevel(distConfigProps.getProperty("logging-level"), Level.WARN)

  /**
   * Number of threads to use in the ExecutionContext while calling DistExtractionJob.run() on multiple
   * extraction jobs in parallel.
   *
   * Note that these threads on the driver node do not perform any heavy work except for executing
   * DistExtractionJob.run() which submits the respective Spark job to the Spark master and waits
   * for the job to finish.
   *
   * By default it is set to Integer.MAX_VALUE so that all extraction jobs are submitted to Spark master
   * simultaneously, which uses the configured scheduling mechanism to execute the jobs on the cluster.
   */
  val extractionJobThreads = distConfigProps.getProperty("extraction-job-threads", Integer.MAX_VALUE.toString).toInt

  /** Whether output files should be overwritten or not (true/false). This is true by default. */
  val overwriteOutput = distConfigProps.getProperty("overwrite-output", "true").toBoolean

  /**
   * Whether the intermediate RDD[WikiPage] should be cached to Hadoop's filesystem (true/false).
   * This is false by default.
   *
   * Performance implications:
   * 1. Caching will make further extractions over the same dump much faster.
   * 2. Caching will force early evaluation of the RDD and will cause some delay before extraction.
   *
   * If you are not planning on repeated extractions over the same dump it is best to leave this as it is.
   */
  val cacheWikiPageRDD = distConfigProps.getProperty("cache-wikipages", "false").toBoolean

  /** Dump directory */
  val dumpDir = getPath("base-dir", pathMustExist = true)

  /** Local ontology file, downloaded for speed and reproducibility */
  val ontologyFile = getPath("ontology", pathMustExist = false)

  /** Local mappings files, downloaded for speed and reproducibility */
  val mappingsDir = getPath("mappings", pathMustExist = false)

  val requireComplete = extractionConfig.requireComplete

  val source = extractionConfig.source

  val disambiguations = extractionConfig.disambiguations

  val wikiName = extractionConfig.wikiName

  val parser = extractionConfig.parser

  val formats = extractionConfig.formats

  val extractorClasses = extractionConfig.extractorClasses

  val namespaces = extractionConfig.namespaces

  /**
   * Creates a Path from the given property (null if the property is absent) and wraps it in an Option.
   * This method first checks the distributed config properties, then the general extraction config properties.
   *
   * @param property String property key
   * @param pathMustExist Boolean to ensure that the Path, if obtained, actually exists.
   * @throws RuntimeException if the property is defined but the path does not exist
   * @return Option wrapping the obtained Path
   */
  def getPath(property: String, pathMustExist: Boolean): Option[Path] =
  {
    val somePath = Option({
                            val distProp = getValue(distConfigProps, property, required = false)(new Path(_))
                            if(distProp != null)
                            {
                              // If property exists in distributed config file return it.
                              distProp
                            }
                            else
                            {
                              // Or else, try the extraction config file - returns either null or a Path.
                              getValue(extractionConfigProps, property, required = false)(new Path(_))
                            }
                          })

    checkPathExists(somePath, pathMustExist)
  }

  /**
   * Custom Config subclass that makes the File-based variables null.
   *
   * The distributed extraction framework should only work with Paths. Initialization operations on non-existent
   * Files may cause errors, and are not required anyway.
   */
  private class ExtractionConfig extends Config(extractionConfigProps)
  {
    override lazy val dumpDir: File = null
    override lazy val ontologyFile: File = null
    override lazy val mappingsDir: File = null
  }

}
