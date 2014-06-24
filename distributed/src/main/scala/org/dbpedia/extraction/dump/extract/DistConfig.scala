package org.dbpedia.extraction.dump.extract

import java.util.Properties
import scala.collection.JavaConversions.asScalaSet
import org.dbpedia.extraction.util.ConfigUtils.getValue
import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.conf.Configuration
import java.io.File

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
class DistConfig(distConfigProps: Properties, extractionConfigProps: Properties)
{
  private val extractionConfig = new ExtractionConfig()

  /** It is recommended that spark-home and spark-master are explicitly provided. */
  val sparkHome = distConfigProps.getProperty("spark-home", sys.env.get("SPARK_HOME").getOrElse(""))

  /** By default assume master is runnning locally; use 4 cores */
  val sparkMaster = distConfigProps.getProperty("spark-master", "local[4]")

  /** Number of splits the initial RDD will be broken to - configure according to your cluster. Maybe total number of cores? */
  val sparkNumSlices = distConfigProps.getProperty("spark-num-slices", "4").toInt

  /** Shows up on Spark Web UI */
  val sparkAppName = distConfigProps.getProperty("spark-appname", "dbpedia-distributed-extraction-framework")

  /** Map of optional spark configuration properties. See http://spark.apache.org/docs/latest/configuration.html */
  val sparkProperties = distConfigProps.stringPropertyNames().filter(_.startsWith("spark")).map(x => (x, distConfigProps.getProperty(x))).toMap

  /** Path to hadoop core-site.xml */
  private val hadoopCoreConf = distConfigProps.getProperty("hadoop-coresite-xml-path")

  /** Path to hadoop hdfs-site.xml */
  private val hadoopHdfsConf = distConfigProps.getProperty("hadoop-hdfssite-xml-path")

  /** Path to hadoop mapred-site.xml */
  private val hadoopMapredConf = distConfigProps.getProperty("hadoop-mapredsite-xml-path")

  /** Hadoop Configuration. This is implicit because RichHadoopPath operations need it. */
  implicit val hadoopConf =
  {
    val hadoopConf = new Configuration()

    if (hadoopCoreConf != null)
      hadoopConf.addResource(new Path(hadoopCoreConf))
    if (hadoopHdfsConf != null)
      hadoopConf.addResource(new Path(hadoopHdfsConf))
    if (hadoopMapredConf != null)
      hadoopConf.addResource(new Path(hadoopMapredConf))

    hadoopConf.set("xmlinput.start", "<page>")
    hadoopConf.set("xmlinput.end", "</page>")

    hadoopConf
  }

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

    // If pathMustExist is set to true, and somePath is defined but it does not exist, throw an error.
    if (pathMustExist && somePath.isDefined && !somePath.get.exists)
    {
      val hadoopHint = if (hadoopCoreConf == null || hadoopHdfsConf == null || hadoopMapredConf == null) " Make sure you configured Hadoop correctly and the directory exists on the configured file system." else ""
      throw sys.error("Dir " + somePath.get + " does not exist." + hadoopHint)
    }
    somePath
  }

  /**
   * Custom Config subclass that makes the File-based variables null.
   *
   * The distributed extraction framework should only work with Paths. Initialization operations on non-existent
   * Files may cause errors, and are not required anyway.
   */
  private class ExtractionConfig extends Config(extractionConfigProps)
  {
    override val dumpDir: File = null
    override val ontologyFile: File = null
    override val mappingsDir: File = null
  }
}
