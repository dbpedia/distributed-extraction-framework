package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.mappings._
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.{Source, WikiPage, XMLSource, WikiSource}
import org.dbpedia.extraction.util._
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.dbpedia.extraction.wikiparser.Namespace
import java.io._
import java.net.URL
import java.util.logging.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.dump.download.Download
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.dbpedia.extraction.spark.io.WikiPageWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.dbpedia.extraction.spark.io.input.DBpediaWikiPageInputFormat

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
  private val CONFIG_PROPERTIES = "config.properties"

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

    // Add input sources
    val job = Job.getInstance(hadoopConfiguration)
    for (file <- files(config.source, finder, date))
      FileInputFormat.addInputPath(job, file)
    hadoopConfiguration = job.getConfiguration // update Configuration

    // Add the extraction configuration file to distributed cache.
    // It will be needed in DBpediaCompositeOutputFormat for getting the Formatters.
    val configPropertiesDCPath = finder.wikiDir.resolve(CONFIG_PROPERTIES) // Path where to the copy config properties file
    val fs = configPropertiesDCPath.getFileSystem(hadoopConfiguration)
    fs.copyFromLocalFile(false, true, new Path(config.extractionConfigFile), configPropertiesDCPath) // Copy local file to Hadoop file system
    job.addCacheFile(configPropertiesDCPath.toUri) // Add to distributed cache

    // Setup config variables needed by DBpediaWikiPageInputFormat and DBpediaCompositeOutputFormat.
    hadoopConfiguration.set("dbpedia.config.properties", configPropertiesDCPath.toString)
    hadoopConfiguration.set("dbpedia.wiki.name", config.wikiName)
    hadoopConfiguration.set("dbpedia.wiki.language.wikicode", lang.wikiCode)
    hadoopConfiguration.set("dbpedia.wiki.date", date)
    hadoopConfiguration.setBoolean("dbpedia.output.overwrite", config.overwriteOutput)

    // Getting the WikiPages from local on-disk cache saves processing time.
    val cache = finder.file(date, "articles-rdd")
    lazy val articlesRDD: RDD[WikiPage] = try
    {
      if (!cache.exists)
        throw new IOException("Cache file " + cache.getSchemeWithFileName + " does not exist.")
      logger.info("Loading articles from cache file " + cache.getSchemeWithFileName)
      val loaded = DistIOUtils.loadRDD(sparkContext, classOf[WikiPage], cache)
      logger.info("WikiPages loaded from cache file " + cache.getSchemeWithFileName)
      loaded
    }
    catch
      {
        case ex: Exception =>
        {
          logger.log(Level.INFO, "Will read from wiki dump file for " + lang.wikiCode + " wiki, could not load cache file '" + cache.getSchemeWithFileName + "': " + ex)

          // Create RDD with WikiPageWritable elements.
          val rawArticlesRDD: RDD[(LongWritable, WikiPageWritable)] =
            sparkContext.newAPIHadoopRDD(hadoopConfiguration, classOf[DBpediaWikiPageInputFormat], classOf[LongWritable], classOf[WikiPageWritable])

          // Unwrap WikiPages and filter unnecessary pages
          val newRdd = rawArticlesRDD.map(_._2.get).filter
                       {
                         page =>
                           page.title.namespace == Namespace.Main ||
                             page.title.namespace == Namespace.File ||
                             page.title.namespace == Namespace.Category ||
                             page.title.namespace == Namespace.Template
                       }.persist(config.sparkStorageLevel)

          if (config.cacheWikiPageRDD)
          {
            DistIOUtils.saveRDD(newRdd, cache)
            logger.info("Parsed WikiPages written to cache file " + cache.getSchemeWithFileName)
          }

          newRdd
        }
      }

    val _ontology =
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

    val _commonsSource =
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

    val _disambiguations =
    {
      val cache = finder.file(date, "disambiguations-ids.obj")
      try
      {
        DistDisambiguations.load(reader(finder.file(date, config.disambiguations)), cache, lang)
      } catch
        {
          case ex: Exception =>
            logger.info("Could not load disambiguations - error: " + ex.getMessage)
            Disambiguations.empty()
        }
    }

    val redirectsCache = finder.file(date, "template-redirects.obj")
    lazy val _redirects = DistRedirects.load(articlesRDD, redirectsCache, lang) // lazy because it will be evaluated in DistExtractionJob.run()

    lazy val context = new DumpExtractionContext
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

      def disambiguations: Disambiguations = if (_disambiguations != null) _disambiguations else new Disambiguations(Set[Long]())
    }

    // Extractors - this is lazily evaluated in DistExtractionJob.run() so that the distributed redirect extraction happens inside run()
    // NOTE: All subsequent references to this val need to be lazy!
    lazy val extractor =
    {
      val _redirects = context.redirects // Trigger evaluation of lazy redirects and load the updated context into extractors.
      val updatedContext = new DumpExtractionContextWrapper(context)
      {
        override def redirects: Redirects = _redirects
      }
      CompositeParseExtractor.load(extractorClasses, updatedContext)
    }

    lazy val destination =
    {
      // Create empty directories for all datasets. This is not strictly necessary because Hadoop would create the directories
      // it needs to by itself, though in that case the directories for unused datasets will obviously be absent.
      val datasets = extractor.datasets
      val outputPath = finder.directory(date)

      for ((suffix, format) <- config.formats; dataset <- datasets)
      {
        new Path(outputPath, s"${finder.wikiName}-$date-${dataset.name.replace('_', '-')}.$suffix").mkdirs()
      }
      new DistMarkerDestination(new DistDeduplicatingWriterDestination(outputPath, hadoopConfiguration), finder.file(date, Extraction.Complete), false)
    }

    lazy val description =
    {
      val datasets = extractor.datasets
      lang.wikiCode + ": " + extractorClasses.size + " extractors (" + extractorClasses.map(_.getSimpleName).mkString(",") + "), " + datasets.size + " datasets (" + datasets.mkString(",") + ")"
    }

    new DistExtractionJob(new RootExtractor(extractor), articlesRDD, config.namespaces, destination, lang.wikiCode, description)
  }

  implicit var hadoopConfiguration: Configuration = config.hadoopConf

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