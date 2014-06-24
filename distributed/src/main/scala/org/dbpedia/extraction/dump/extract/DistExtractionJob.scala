package org.dbpedia.extraction.dump.extract

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.destinations.{Quad, Destination}
import org.dbpedia.extraction.mappings.RootExtractor
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.Namespace
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.util.SparkUtils
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
class DistExtractionJob(val extractor: RootExtractor, val rdd: RDD[WikiPage], val namespaces: Set[Namespace], destination: Destination, label: String, description: String)
{
  val logger = Logger.getLogger(getClass.getName)

  val progress = new ExtractionProgress(label, description)

  def run(): Unit =
  {
    progress.start()
    destination.open()

    // Wrap the ExtractorMapper to make the closure Kryo-serialized.
    val mapper = SparkUtils.kryoWrapFunction(new ExtractorMapper(extractor, namespaces))

    // Map the RDD[WikiPage] into the resulting RDD.
    val results: RDD[Seq[Quad]] = rdd.map(mapper)

    // Iterate locally and write into destination (local FS, HDFS etc. according to Hadoop's Configuration).
    // TODO: Need a better method for writing into destination. This one currently streams all results to the master while writing to the destination.
    // TODO: Solution: Write a Hadoop OutputFormat which Spark can use to write outputs in parallel.
    val iter = SparkUtils.rddToLocalIterator(results)
    while (iter.hasNext)
    {
      progress.countPage(true)
      destination.write(iter.next())
    }

    destination.close()
    progress.end()
  }
}


/**
 * This is the actual mapper that takes a WikiPage, performs the extraction with the set of extractors
 * and returns the results as a Seq[Quad]. This is similar to what happens in ExtractionJob.
 *
 * @param extractor
 * @param namespaces
 */
class ExtractorMapper(val extractor: RootExtractor, val namespaces: Set[Namespace]) extends (WikiPage => Seq[Quad])
{

  private lazy val logger = Logger.getLogger(getClass.getName)

  override def apply(page: WikiPage): Seq[Quad] =
  {
    var success = false
    var graph: Seq[Quad] = null
    try
    {
      if (namespaces.contains(page.title.namespace))
      {
        graph = extractor(page)
      }
      success = true
    } catch
      {
        case ex: Exception => logger.log(Level.WARNING, "error processing page '" + page.title + "': " + Exceptions.toString(ex, 200))
      }
    if (graph != null)
      graph
    else
      Nil
  }
}