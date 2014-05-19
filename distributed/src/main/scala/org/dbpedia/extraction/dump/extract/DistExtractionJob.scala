package org.dbpedia.extraction.dump.extract

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.destinations.{Quad, Destination}
import org.dbpedia.extraction.mappings.RootExtractor
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.Namespace
import org.dbpedia.util.Exceptions
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.spark.serialize.KryoSerializationWrapper

/**
 * Executes a extraction using Spark.
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

    val results: RDD[Seq[Quad]] = rdd.map(DistExtractionJob.mapper(extractor, namespaces))
    //FIXME: This collects all results to driver node - may run out of memory for large result sets.
    //Use foreach or something like that.
    val quads = results.collect()
    //FIXME: Progress will give wrong time values because main processing is done in bulk parallel before counting pages.
    //Using foreach will fix this. Need to figure it out. Causing serialization problems.
    for (i <- quads)
    {
      progress.countPage(true)
      destination.write(i)
    }

    destination.close()
    progress.end()
  }
}

object DistExtractionJob
{
  def genMapper[T, U](kryoWrapper: KryoSerializationWrapper[(T => U)])
                     (page: T): U =
  {
    kryoWrapper.value.apply(page)
  }

  def mapper(extractor: RootExtractor, namespaces: Set[Namespace]) =
  {
    genMapper[WikiPage, Seq[Quad]](KryoSerializationWrapper(new ExtractorMapper(extractor, namespaces))) _
  }
}


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