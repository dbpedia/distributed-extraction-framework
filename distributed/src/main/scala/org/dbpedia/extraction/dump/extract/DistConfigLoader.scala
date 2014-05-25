package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.destinations.CompositeDestination
import org.dbpedia.extraction.destinations.DatasetDestination
import org.dbpedia.extraction.destinations.DeduplicatingDestination
import org.dbpedia.extraction.destinations.Destination
import org.dbpedia.extraction.destinations.MarkerDestination
import org.dbpedia.extraction.destinations.WriterDestination
import org.dbpedia.extraction.mappings._
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.{Source, WikiPage, XMLSource, WikiSource}
import org.dbpedia.extraction.util._
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.wikiparser.Namespace
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.io._
import java.net.URL
import java.util.logging.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.dump.download.Download

/**
 * Loads the dump extraction configuration.
 *
 * This class configures Spark and sets up the extractors to run using Spark
 *
 * TODO: get rid of all config file parsers, use Spring
 * TODO: Inherit ConfigLoader methods and get rid of redundant code
 *
 * @param config General extraction framework configuration
 * @param distConfig Distributed configuration
 */
class DistConfigLoader(config: Config, distConfig: DistConfig) extends ConfigLoader(config)
{
  private val logger = Logger.getLogger(classOf[DistConfigLoader].getName)

  /**
   * Loads the configuration and creates extraction jobs for all configured languages.
   *
   * @return Non-strict Traversable over all configured extraction jobs i.e. an extractions job will not be created until it is explicitly requested.
   */
  override def getExtractionJobs(): Traversable[DistExtractionJob] =
  {
    // Create a non-strict view of the extraction jobs
    // non-strict because we want to create the extraction job when it is needed, not earlier
    config.extractorClasses.view.map(e => createExtractionJob(e._1, e._2))
  }

  /**
   * Creates ab extraction job for a specific language.
   */
  private def createExtractionJob(lang: Language, extractorClasses: List[Class[_ <: Extractor[_]]]): DistExtractionJob =
  {
    val finder = new Finder[File](config.dumpDir, lang, config.wikiName)

    val date = latestDate(finder)

    val _articlesSource =
    {
      val articlesReaders = readers(config.source, finder, date)

      XMLSource.fromReaders(articlesReaders, lang,
                            title => title.namespace == Namespace.Main || title.namespace == Namespace.File ||
                              title.namespace == Namespace.Category || title.namespace == Namespace.Template)
    }

    SparkUtils.silenceSpark()
    val sparkContext = SparkUtils.getSparkContext(distConfig)

    val cache = finder.file(date, "articles-rdd")

    // Getting the WikiPages from local on-disk cache saves processing time.
    val articlesRDD: RDD[WikiPage] = try
    {
      logger.info("Loading articles from cache file " + cache)
      val loaded = DistIOUtils.loadRDD(sparkContext, classOf[WikiPage], cache.getAbsolutePath)
      // count() throws org.apache.hadoop.mapred.InvalidInputException if file doesn't exist
      val count = loaded.count()
      logger.info(count + " WikiPages loaded from cache file " + cache)
      loaded
    }
    catch
      {
        case ex: Exception =>
        {
          logger.log(Level.INFO, "Will read from wiki dump file for " + lang.wikiCode + " wiki, could not load cache file '" + cache + "': " + ex)
          val articlesReaders = readers(config.source, finder, date)

          val newRdd = sparkContext.parallelize(_articlesSource.toSeq, distConfig.sparkNumSlices)

          DistIOUtils.saveRDD(newRdd, cache.getAbsolutePath)
          newRdd
        }
      }

    val _redirects =
    {
      val cache = finder.file(date, "template-redirects.obj")
      DistRedirects.load(articlesRDD, cache, lang)
    }

    val contextBroadcast = sparkContext.broadcast(new DumpExtractionContext
    {
      def ontology = _ontology

      def commonsSource = _commonsSource

      def language = lang

      private lazy val _mappingPageSource =
      {
        val namespace = Namespace.mappings(language)

        if (config.mappingsDir != null && config.mappingsDir.isDirectory)
        {
          val file = new File(config.mappingsDir, namespace.name(Language.Mappings).replace(' ', '_') + ".xml")
          XMLSource.fromFile(file, Language.Mappings)
        }
        else
        {
          val namespaces = Set(namespace)
          val url = new URL(Language.Mappings.apiUri)
          WikiSource.fromNamespaces(namespaces, url, Language.Mappings)
        }
      }

      def mappingPageSource: Traversable[WikiPage] = _mappingPageSource

      private lazy val _mappings =
      {
        MappingsLoader.load(this)
      }

      def mappings: Mappings = _mappings

      def articlesSource: Source = _articlesSource

      def redirects: Redirects = _redirects

      private val _disambiguations =
      {
        val cache = finder.file(date, "disambiguations-ids.obj")
        try
        {
          Disambiguations.load(reader(finder.file(date, config.disambiguations)), cache, language)
        } catch
          {
            case ex: Exception =>
              logger.info("Could not load disambiguations - error: " + ex.getMessage)
              null
          }
      }

      def disambiguations: Disambiguations = if (_disambiguations != null) _disambiguations else new Disambiguations(Set[Long]())

    })

    val context = new DistDumpExtractionContext(contextBroadcast)

    //Extractors
    val extractor = CompositeParseExtractor.load(extractorClasses, context)
    val datasets = extractor.datasets

    val formatDestinations = new ArrayBuffer[Destination]()
    for ((suffix, format) <- config.formats)
    {

      val datasetDestinations = new HashMap[String, Destination]()
      for (dataset <- datasets)
      {
        val file = finder.file(date, dataset.name.replace('_', '-') + '.' + suffix)
        datasetDestinations(dataset.name) = new DeduplicatingDestination(new WriterDestination(writer(file), format))
      }

      formatDestinations += new DatasetDestination(datasetDestinations)
    }

    val destination = new MarkerDestination(new CompositeDestination(formatDestinations.toSeq: _*), finder.file(date, Extraction.Complete), false)

    val description = lang.wikiCode + ": " + extractorClasses.size + " extractors (" + extractorClasses.map(_.getSimpleName).mkString(",") + "), " + datasets.size + " datasets (" + datasets.mkString(",") + ")"
    new DistExtractionJob(new RootExtractor(extractor), articlesRDD, config.namespaces, destination, lang.wikiCode, description)
  }

  // TODO: Inherit methods below from ConfigLoader?

  //language-independent val
  private val _ontology =
  {
    val ontologySource = if (config.ontologyFile != null && config.ontologyFile.isFile)
    {
      XMLSource.fromFile(config.ontologyFile, Language.Mappings)
    }
    else
    {
      val namespaces = Set(Namespace.OntologyClass, Namespace.OntologyProperty)
      val url = new URL(Language.Mappings.apiUri)
      val language = Language.Mappings
      WikiSource.fromNamespaces(namespaces, url, language)
    }

    new OntologyReader().read(ontologySource)
  }

  //language-independent val
  private val _commonsSource =
  {
    try
    {
      val finder = new Finder[File](config.dumpDir, Language("commons"), config.wikiName)
      val date = latestDate(finder)
      XMLSource.fromReaders(readers(config.source, finder, date), Language.Commons, _.namespace == Namespace.File)
    } catch
      {
        case ex: Exception =>
          logger.info("Could not load disambiguations - error: " + ex.getMessage)
          null
      }
  }


  private def writer(file: File): () => Writer =
  {
    () => IOUtils.writer(file)
  }

  private def reader(file: File): () => Reader =
  {
    () => IOUtils.reader(file)
  }

  private def readers(source: String, finder: Finder[File], date: String): List[() => Reader] =
  {
    files(source, finder, date).map(reader(_))
  }

  private def files(source: String, finder: Finder[File], date: String): List[File] =
  {

    val files = if (source.startsWith("@"))
    {
      // the articles source is a regex - we want to match multiple files
      finder.matchFiles(date, source.substring(1))
    } else List(finder.file(date, source))

    logger.info(s"Source is ${source} - ${files.size} file(s) matched")

    files
  }

  private def latestDate(finder: Finder[_]): String =
  {
    val isSourceRegex = config.source.startsWith("@")
    val source = if (isSourceRegex) config.source.substring(1) else config.source
    val fileName = if (config.requireComplete) Download.Complete else source
    finder.dates(fileName, isSuffixRegex = isSourceRegex).last
  }
}