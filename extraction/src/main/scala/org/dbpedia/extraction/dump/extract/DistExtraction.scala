package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.util.{SparkUtils, ProxyAuthenticator, ConfigUtils}
import java.net.Authenticator
import scala.concurrent.{ExecutionContext, Await, Future, future}
import scala.concurrent.duration.Duration
import java.io.File
import java.util.concurrent.Executors

/**
 * Dump extraction script.
 */
object DistExtraction
{

  val Started = "extraction-started"

  val Complete = "extraction-complete"

  def main(args: Array[String]): Unit =
  {
    require(args != null && args.length >= 2 && args(0).nonEmpty && args(1).nonEmpty, "missing required arguments: <extraction config file name> <spark config file name>")
    Authenticator.setDefault(new ProxyAuthenticator())

    // Load properties
    val extractionConfigProps = ConfigUtils.loadConfig(args(0), "UTF-8")
    val distConfigProps = ConfigUtils.loadConfig(args(1), "UTF-8")
    val distConfig = new DistConfig(distConfigProps, extractionConfigProps, new File(args(0)).toURI)

    // overwrite properties with CLI args
    // TODO arguments could be of the format a=b and then property a can be overwritten with "b"

    // Create SparkContext
    SparkUtils.setSparkLogLevels(distConfig)
    val sparkContext = SparkUtils.getSparkContext(distConfig)

    // Load extraction jobs from configuration
    val jobs = new DistConfigLoader(distConfig, sparkContext).getExtractionJobs()

    val executor = Executors.newFixedThreadPool(distConfig.extractionJobThreads)
    implicit val ec = ExecutionContext.fromExecutor(executor)
    val futures = for (job <- jobs) yield future
                                          {
                                            job.run()
                                          }
    Await.result(Future.sequence(futures), Duration.Inf)

    sparkContext.stop()
    executor.shutdown()
  }
}
