package org.dbpedia.extraction.mappings

import org.junit.Assert._
import org.dbpedia.extraction.sources.{Source, XMLSource, WikiPage}
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.util._
import java.io.File
import org.dbpedia.extraction.wikiparser.Namespace
import org.dbpedia.extraction.dump.extract.{Config, DistConfig}
import org.dbpedia.extraction.dump.download.Download
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath

/**
 * Unit Test for the DistRedirects class.
 *
 * This test expects a DBpedia extraction configuration properties file named "config.properties" and a distributed
 * framework configuration file named "dist-config.properties" to be present at the test/resources directory.
 *
 * It's better to use a small data dump like the liwiki dump to run the test.
 *
 * TODO: Add some wiki dump content to test upon rather than rely upon an external wiki dump file and config files.
 */
@RunWith(classOf[JUnitRunner])
class DistRedirectsTest extends FunSuite
{
  val CONFIG_FILE = "config.properties"
  val SPARK_CONFIG_FILE = "dist-config.properties"

  // Fixtures shared between all tests in this class
  val (distConfig: DistConfig,
  articleSource: Source,
  rdd: RDD[WikiPage],
  language: Language,
  date: String,
  distFinder: Finder[Path]) = try
  {
    val configFileResource = getClass.getClassLoader.getResource(CONFIG_FILE)
    val sparkConfigFileResource = getClass.getClassLoader.getResource(SPARK_CONFIG_FILE)

    //Check if the wiki-pages file and config.properties file are present
    assertNotNull("Test file %s missing from distributed/src/test/resources".format(CONFIG_FILE), configFileResource)
    assertNotNull("Test file %s missing from distributed/src/test/resources".format(SPARK_CONFIG_FILE), sparkConfigFileResource)

    val configProperties = ConfigUtils.loadConfig(configFileResource.toURI.getPath, "UTF-8")
    val distConfigProperties = ConfigUtils.loadConfig(sparkConfigFileResource.toURI.getPath, "UTF-8")
    val config = new Config(configProperties)
    val distConfig = new DistConfig(distConfigProperties, configProperties, configFileResource.toURI)
    implicit val hadoopConfiguration = distConfig.hadoopConf
    val lang = config.extractorClasses.iterator.next()._1

    val localFinder = new Finder[File](config.dumpDir, lang, config.wikiName)
    val distFinder = new Finder[Path](distConfig.dumpDir.get, lang, config.wikiName)
    val date = latestDate(config, localFinder)

    // Get the readers for the test dump files
    val articlesReaders = files(config.source, localFinder, date).map(x => () => IOUtils.reader(x))

    // Get the article source for Redirects to load from
    val articleSource = XMLSource.fromReaders(articlesReaders, lang,
                                              title => title.namespace == Namespace.Main || title.namespace == Namespace.File ||
                                                title.namespace == Namespace.Category || title.namespace == Namespace.Template)

    SparkUtils.setSparkLogLevels(distConfig)
    val sc = SparkUtils.getSparkContext(distConfig)
    // Generate RDD from the article source for DistRedirects to load from in parallel
    // Naively calls toArray on Seq, only for testing
    val rdd = sc.parallelize(articleSource.toSeq, 8)
    (distConfig, articleSource, rdd, lang, date, distFinder)
  } catch{ case ex:Exception => ex.printStackTrace(); (null, null,null, null,null, null)}

  implicit def hadoopConfiguration: Configuration = distConfig.hadoopConf

  test("Verify DistRedirects.loadFromRDD output")
  {
    val distRedirects = DistRedirects.loadFromRDD(rdd, language)
    val redirects = Redirects.loadFromSource(articleSource, language)
    assertEquals("Testing DistRedirects.loadFromRDD failed!", redirects.map, distRedirects.map)
  }

  test("Verify DistRedirects.load output")
  {
    val cache = distFinder.file(date, "template-redirects.obj")
    var distRedirects = DistRedirects.load(rdd, cache, language)
    var redirects = Redirects.loadFromSource(articleSource, language)
    assertEquals("Testing DistRedirects.loadFromRDD failed!", redirects.map, distRedirects.map)

    // Try again so that cache gets used
    distRedirects = DistRedirects.load(rdd, cache, language)
    redirects = Redirects.loadFromSource(articleSource, language)
    assertEquals("Testing DistRedirects.loadFromRDD failed!", redirects.map, distRedirects.map)
  }

  // Taken from org.dbpedia.extraction.dump.extract.Config
  def latestDate(config: Config, finder: Finder[_]): String =
  {
    val isSourceRegex = config.source.startsWith("@")
    val source = if (isSourceRegex) config.source.substring(1) else config.source
    val fileName = if (config.requireComplete) Download.Complete else source
    finder.dates(fileName, isSuffixRegex = isSourceRegex).last
  }

  // Taken from org.dbpedia.extraction.dump.extract.Config
  def files(source: String, finder: Finder[File], date: String): List[File] =
  {

    val files = if (source.startsWith("@"))
    {
      // the articles source is a regex - we want to match multiple files
      finder.matchFiles(date, source.substring(1))
    } else List(finder.file(date, source))

    files
  }
}