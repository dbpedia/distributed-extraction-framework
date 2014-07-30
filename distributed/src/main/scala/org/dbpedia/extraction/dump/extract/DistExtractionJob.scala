package org.dbpedia.extraction.dump.extract

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.destinations.{Quad, DistDestination}
import org.dbpedia.extraction.mappings.RootExtractor
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.Namespace
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.util.StringUtils
import org.apache.spark.SparkContext._
import org.dbpedia.util.Exceptions

/**
 * Executes an extraction using Spark.
 *
 * @param extractor The Extractor
 * @param rdd The RDD of WikiPages
 * @param namespaces Only extract pages in these namespaces
 * @param destination The extraction destination. Will be closed after the extraction has been finished.
 * @param label user readable label of this extraction job.
 */
class DistExtractionJob(extractor: => RootExtractor, rdd: => RDD[WikiPage], namespaces: Set[Namespace], destination: => DistDestination, label: String, description: => String)
{
  private val logger = Logger.getLogger(getClass.getName)

  def run(): Unit =
  {
    val sc = rdd.sparkContext
    val allPages = sc.accumulator(0)
    val failedPages = sc.accumulator(0)

    val loggerBC = sc.broadcast(logger)
    val extractorBC = sc.broadcast(extractor)
    val namespacesBC = sc.broadcast(namespaces)

    val startTime = System.currentTimeMillis

    val results: RDD[Seq[Quad]] =
      rdd.map
      {
        page =>
          // Take a WikiPage, perform the extraction with a set of extractors and return the results as a Seq[Quad].
          val (success, graph) = try
          {
            (true, if (namespacesBC.value.contains(page.title.namespace)) Some(extractorBC.value.apply(page)) else None)
          }
          catch
            {
              case ex: Exception =>
                loggerBC.value.log(Level.WARNING, "error processing page '" + page.title + "': " + Exceptions.toString(ex, 200))
                (false, None)
            }

          if (success) allPages += 1 else failedPages += 1

          graph.getOrElse(Nil)
      }

    logger.info(description+" started")

    destination.open()

    logger.info("Writing outputs to destination...")

    destination.write(results)

    destination.close()

    val time = System.currentTimeMillis - startTime
    println("%s: extracted %d pages in %s (per page: %f ms; failed pages: %d).".format(label,
                                                                                       allPages.value,
                                                                                       StringUtils.prettyMillis(time),
                                                                                       time.toDouble / allPages.value,
                                                                                       failedPages.value))

    logger.info(description+" finished")
  }
}
