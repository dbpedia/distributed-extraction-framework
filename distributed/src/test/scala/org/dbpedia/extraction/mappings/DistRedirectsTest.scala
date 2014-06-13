package org.dbpedia.extraction.mappings

import org.junit.Test
import org.junit.Assert._
import org.dbpedia.extraction.sources.{Source, XMLSource, WikiPage}
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.util._
import java.io.File
import org.dbpedia.extraction.wikiparser.Namespace
import org.dbpedia.extraction.dump.extract.{Config, DistConfig}
import org.dbpedia.extraction.dump.download.Download
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.{Logger, Level}

@RunWith(classOf[JUnitRunner])
class DistRedirectsTest extends FunSuite
{
  val CONFIG_FILE = "config.properties"
  val SPARK_CONFIG_FILE = "spark-config.properties"

  // Fixtures shared between all tests in this class
  val (articleSource: Source,
  rdd: RDD[WikiPage],
  language: Language,
  date: String,
  finder: Finder[File],
  sparkContext: SparkContext) =
  {
    val configFileResource = getClass.getClassLoader.getResource(CONFIG_FILE)
    val sparkConfigFileResource = getClass.getClassLoader.getResource(SPARK_CONFIG_FILE)

    //Check if the wiki-pages file and config.properties file are present
    assertNotNull("Test file %s missing from distributed/src/test/resources".format(CONFIG_FILE), configFileResource)
    assertNotNull("Test file %s missing from distributed/src/test/resources".format(SPARK_CONFIG_FILE), sparkConfigFileResource)

    val config = new Config(ConfigUtils.loadConfig(configFileResource.toURI.getPath, "UTF-8"))
    val distConfig = new DistConfig(ConfigUtils.loadConfig(sparkConfigFileResource.toURI.getPath, "UTF-8"))
    val lang = config.extractorClasses.iterator.next()._1

    val finder = new Finder[File](config.dumpDir, lang, config.wikiName)
    val date = latestDate(config, finder)

    // Get the readers for the test dump files
    val articlesReaders = files(config.source, finder, date).map(x => () => IOUtils.reader(x))

    // Get the article source for Redirects to load from
    val articleSource = XMLSource.fromReaders(articlesReaders, lang,
                                              title => title.namespace == Namespace.Main || title.namespace == Namespace.File ||
                                                title.namespace == Namespace.Category || title.namespace == Namespace.Template)

    SparkUtils.silenceSpark()
    val sc = SparkUtils.getSparkContext(distConfig)
    // Generate RDD from the article source for DistRedirects to load from in parallel
    // Naively calls toArray on Seq, only for testing
    val rdd = sc.parallelize(articleSource.toSeq)
    (articleSource, rdd, lang, date, finder, sc)
  }

  test("Verify DistRedirects.loadFromRDD output")
  {
    val distRedirects = DistRedirects.loadFromRDD(rdd, language)
    val redirects = Redirects.loadFromSource(articleSource, language)
    assertEquals("Testing DistRedirects.loadFromRDD failed!", redirects.map, distRedirects.map)
  }

  test("Verify DistRedirects.load output")
  {
    val cache = finder.file(date, "template-redirects.obj")
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