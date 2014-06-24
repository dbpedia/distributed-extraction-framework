package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.util.{SparkUtils, ProxyAuthenticator, ConfigUtils}
import java.net.Authenticator
import scala.concurrent.{Await, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

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
    val distConfig = new DistConfig(distConfigProps, extractionConfigProps)

    // overwrite properties with CLI args
    // TODO arguments could be of the format a=b and then property a can be overwritten with "b"

    // Create SparkContext
    SparkUtils.silenceSpark()
    val sparkContext = SparkUtils.getSparkContext(distConfig)

    // Load extraction jobs from configuration
    val jobs = new DistConfigLoader(distConfig, sparkContext).getExtractionJobs()

    // Execute the extraction jobs in parallel using the default ExecutionContext
    // TODO: Equip the framework with an OutputFormat or something so that DistExtractionJob can write output using Hadoop's API, in a distributed manner.
    // TODO: Probably use a custom ExecutionContext and a non-blocking approach so that number of simultaneous jobs is NOT limited by driver/master node's CPU/threads.
    val futures = for (job <- jobs) yield future
                                          {
                                            job.run()
                                          }
    Await.result(Future.sequence(futures), Duration.Inf)

    sparkContext.stop()
  }
}
