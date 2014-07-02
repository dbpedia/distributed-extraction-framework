package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.mappings._
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.{Source, WikiPage, XMLSource, WikiSource}
import org.dbpedia.extraction.util._
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.dbpedia.extraction.wikiparser.Namespace
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.io._
import java.net.URL
import java.util.logging.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.dump.download.Download
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import scala.xml.XML
import org.dbpedia.extraction.spark.io.XmlInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

/**
 * Loads the dump extraction configuration.
 *
 * This class configures Spark and sets up the extractors to run using Spark
 *
 * TODO: get rid of all config file parsers, use Spring
 * TODO: Inherit ConfigLoader methods and get rid of redundant code
 *
 * @param config DistConfig
 */
class DistConfigLoader(config: DistConfig, sparkContext: SparkContext)
{
  private val logger = Logger.getLogger(classOf[DistConfigLoader].getName)

  /**
   * Loads the configuration and creates extraction jobs for all configured languages.
   *
   * @return Non-strict Traversable over all configured extraction jobs i.e. an extractions job will not be created until it is explicitly requested.
   */
  def getExtractionJobs(): Traversable[DistExtractionJob] =
  {
    // Create a non-strict view of the extraction jobs
    // non-strict because we want to create the extraction job when it is needed, not earlier
    config.extractorClasses.view.map(e => createExtractionJob(e._1, e._2))
  }

  /**
   * Creates an extraction job for a specific language.
   */
  private def createExtractionJob(lang: Language, extractorClasses: Seq[Class[_ <: Extractor[_]]]): DistExtractionJob =
  {
    val dumpDir = config.dumpDir.get

    // Finder[Path] works with Hadoop's FileSystem class - operates on HDFS, or the local file system depending
    // upon whether we are running in local mode or distributed/cluster mode.
    val finder = new Finder[Path](dumpDir, lang, config.wikiName)
    val date = latestDate(finder)

    // Getting the WikiPages from local on-disk cache saves processing time.
    val cache = finder.file(date, "articles-rdd")
    val articlesRDD: RDD[WikiPage] = try
    {
      logger.info("Loading articles from cache file " + cache)
      val loaded = DistIOUtils.loadRDD(sparkContext, classOf[WikiPage], cache)
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

          // Add input sources
          val job = Job.getInstance(hadoopConfiguration)
          for (file <- files(config.source, finder, date))
            FileInputFormat.addInputPath(job, file)

          val updatedConf = job.getConfiguration

          // Create RDD with <page>...</page> elements.
          val rawArticlesRDD: RDD[(LongWritable, Text)] =
            sparkContext.newAPIHadoopRDD(updatedConf, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])

          // Function to parse a <page>...</page> string into a WikiPage.
          val wikiPageParser: (((LongWritable, Text)) => WikiPage) =
            keyValue => XMLSource.fromXML(XML.loadString("<mediawiki>" + keyValue._2.toString + "</mediawiki>"), lang).toSeq.head
          val mapper = SparkUtils.kryoWrapFunction(wikiPageParser)

          val newRdd = rawArticlesRDD.map(mapper).filter
                       {
                         page =>
                           page.title.namespace == Namespace.Main ||
                             page.title.namespace == Namespace.File ||
                             page.title.namespace == Namespace.Category ||
                             page.title.namespace == Namespace.Template
                       }.cache()

          DistIOUtils.saveRDD(newRdd, cache)
          logger.info("Parsed WikiPages written to cache file " + cache)
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

        config.mappingsDir match
        {
          case Some(mappingsDir) if mappingsDir.isDirectory =>
            // Is mappingsDir defined and it is indeed a directory?
            val path = new Path(mappingsDir, namespace.name(Language.Mappings).replace(' ', '_') + ".xml")
            XMLSource.fromReader(reader(path), Language.Mappings)
          case _ =>
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

      def articlesSource: Source = null // Not needing raw article source

      def redirects: Redirects = _redirects

      private val _disambiguations =
      {
        val cache = finder.file(date, "disambiguations-ids.obj")
        try
        {
          DistDisambiguations.load(reader(finder.file(date, config.disambiguations)), cache, language)
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

    // Extractors
    val extractor = CompositeParseExtractor.load(extractorClasses, context)
    val datasets = extractor.datasets

    // Setup destinations for each dataset
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

  implicit def hadoopConfiguration: Configuration = config.hadoopConf

  //language-independent val
  private val _ontology =
  {
    val ontologySource = config.ontologyFile match
    {
      case Some(ontologyFile) if ontologyFile.isFile =>
        // Is ontologyFile defined and it is indeed a file?
        XMLSource.fromReader(reader(ontologyFile), Language.Mappings)
      case _ =>
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
      val finder = new Finder[Path](config.dumpDir.get, Language("commons"), config.wikiName)
      val date = latestDate(finder)
      XMLSource.fromReaders(readers(config.source, finder, date), Language.Commons, _.namespace == Namespace.File)
    }
    catch
      {
        case ex: Exception =>
          logger.info("Could not load commons source - error: " + ex.getMessage)
          null
      }
  }


  private def writer[T <% FileLike[_]](file: T): () => Writer =
  {
    () => IOUtils.writer(file)
  }

  private def reader[T <% FileLike[_]](file: T): () => Reader =
  {
    () => IOUtils.reader(file)
  }

  private def readers[T <% FileLike[_]](source: String, finder: Finder[T], date: String): List[() => Reader] =
  {
    files(source, finder, date).map(reader(_))
  }

  private def files[T <% FileLike[_]](source: String, finder: Finder[T], date: String): List[T] =
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