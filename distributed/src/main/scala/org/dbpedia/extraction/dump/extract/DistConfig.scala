package org.dbpedia.extraction.dump.extract

import java.util.Properties
import scala.collection.JavaConversions.asScalaSet
import org.dbpedia.extraction.util.ConfigUtils.getValue
import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.conf.Configuration

/**
 * Class for distributed configuration
 */
class DistConfig(config: Properties)
{
  /** It is recommended that spark-home and spark-master are explicitly provided. */
  val sparkHome = config.getProperty("spark-home", sys.env.get("SPARK_HOME").getOrElse(""))

  /** By default assume master is runnning locally; use 4 cores */
  val sparkMaster = config.getProperty("spark-master", "local[4]")

  /** Number of splits the initial RDD will be broken to - configure according to your cluster. Maybe total number of cores? */
  val sparkNumSlices = config.getProperty("spark-num-slices", "4").toInt

  /** Shows up on Spark Web UI */
  val sparkAppName = config.getProperty("spark-appname", "dbpedia-distributed-extraction-framework")

  /** Map of optional spark configuration properties. See http://spark.apache.org/docs/latest/configuration.html */
  val sparkProperties = config.stringPropertyNames().filter(_.startsWith("spark")).map(x => (x, config.getProperty(x))).toMap

  /** Path to hadoop core-site.xml */
  private val hadoopCoreConf = config.getProperty("hadoop-coresite-xml-path")

  /** Path to hadoop hdfs-site.xml */
  private val hadoopHdfsConf = config.getProperty("hadoop-hdfssite-xml-path")

  /** Path to hadoop mapred-site.xml */
  private val hadoopMapredConf = config.getProperty("hadoop-mapredsite-xml-path")

  /** Hadoop Configuration. This is implicit because RichHadoopPath operations need it. */
  implicit val hadoopConf =
  {
    val hadoopConf = new Configuration()

    if (hadoopCoreConf != null)
      hadoopConf.addResource(new Path(hadoopCoreConf))
    if (hadoopHdfsConf  != null)
      hadoopConf.addResource(new Path(hadoopHdfsConf))
    if (hadoopMapredConf != null)
      hadoopConf.addResource(new Path(hadoopMapredConf))

    hadoopConf.set("xmlinput.start", "<page>")
    hadoopConf.set("xmlinput.end", "</page>")

    // Set max input split size to ~10mb if not set
    if (null == hadoopConf.get("mapred.max.split.size"))
      hadoopConf.set("mapred.max.split.size", "10000000")

    hadoopConf
  }

  /** Dump directory. Note that Config.dumpDir is ignored in the distributed framework. */
  val dumpDir = getValue(config, "base-dir", required = true)(new Path(_))
  if (!dumpDir.exists)
  {
    val hadoopHint = if (hadoopCoreConf == null || hadoopHdfsConf == null || hadoopMapredConf == null) " Make sure you configured Hadoop correctly and the directory exists on the configured file system." else ""
    throw sys.error("Dir " + dumpDir + " does not exist." + hadoopHint)
  }
}
