package org.dbpedia.extraction.mappings

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.sources.WikiPage
import java.io._
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.util.{DistIOUtils, Language}
import org.dbpedia.extraction.wikiparser.impl.wikipedia.Redirect
import org.apache.spark.rdd.RDD
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext._

/**
 * Distributed version of Redirects; uses Spark to compute redirects.
 *
 * Holds the redirects between wiki pages
 * At the moment, only redirects between Templates are considered
 *
 * @param map Redirect map. Contains decoded template titles.
 *
 * @see Redirects
 */
class DistRedirects(override val map: Map[String, String]) extends Redirects(map)

/**
 * Loads redirects from a cache file or source of Wiki pages.
 * At the moment, only redirects between Templates are considered
 */
object DistRedirects
{
  private val logger = Logger.getLogger(classOf[DistRedirects].getName)

  /**
   * Tries to load the redirects from a cache file.
   * If not successful, loads the redirects from an RDD.
   * Updates the cache after loading the redirects from the source.
   *
   * @param rdd RDD of WikiPages
   * @param cache Path to cache file
   * @param lang Language
   * @param hadoopConf Configuration
   * @return Redirects object
   */
  def load(rdd: RDD[WikiPage], cache: Path, lang: Language)(implicit hadoopConf: Configuration): Redirects =
  {
    //Try to load redirects from the cache
    try
    {
      return loadFromCache(cache)
    }
    catch
      {
        case ex: Exception => logger.log(Level.INFO, "Will extract redirects from source for " + lang.wikiCode + " wiki, could not load cache file '" + cache.getSchemeWithFileName + "': " + ex)
      }

    //Load redirects from RDD
    val redirects = loadFromRDD(rdd, lang)

    val dir = cache.getParent
    if (!dir.exists && !dir.mkdirs()) throw new IOException("cache dir [" + dir.getSchemeWithFileName + "] does not exist and cannot be created")
    val output = new Output(new BufferedOutputStream(cache.outputStream()))
    try
    {
      DistIOUtils.getKryoInstance.writeClassAndObject(output, redirects.map)
      logger.info(redirects.map.size + " redirects written to cache file " + cache.getSchemeWithFileName)
      redirects
    }
    finally
    {
      output.close()
    }
  }

  /**
   * Loads the redirects from a cache file.
   */
  private def loadFromCache(cache: Path)(implicit hadoopConf: Configuration): Redirects =
  {
    logger.info("Loading redirects from cache file " + cache.getSchemeWithFileName)
    val input = new Input(new BufferedInputStream(cache.inputStream()))
    try
    {
      val redirects = new Redirects(DistIOUtils.getKryoInstance.readClassAndObject(input).asInstanceOf[Map[String, String]])
      logger.info(redirects.map.size + " redirects loaded from cache file " + cache.getSchemeWithFileName)
      redirects
    }
    finally
    {
      input.close()
    }
  }

  /**
   * Loads the redirects from a source.
   *
   * @param rdd RDD of WikiPages
   * @param lang Language
   * @return Redirects object
   */
  def loadFromRDD(rdd: RDD[WikiPage], lang: Language): Redirects =
  {
    logger.info("Loading redirects from source (" + lang.wikiCode + ")")

    val regexBC = rdd.sparkContext.broadcast(buildRegex(lang))

    // Wrap the map function inside a KryoSerializationWrapper
    //    val mapper = SparkUtils.kryoWrapFunction(new RedirectFinder(langBC))
    //    val redirects = new Redirects(rdd.flatMap(mapper).collectAsMap().toMap)

    val redirectsRDD = rdd.flatMap
                       {
                         case page: WikiPage =>
                           val regex = regexBC.value

                           val destinationTitle = page.source match
                           {
                             case regex(destination) =>
                               try
                               {
                                 WikiTitle.parse(destination, page.title.language)
                               }
                               catch
                                 {
                                   case ex: WikiParserException =>
                                     Logger.getLogger(Redirects.getClass.getName).log(Level.WARNING, "Couldn't parse redirect destination", ex)
                                     null
                                 }
                             case _ => null
                           }

                           if (destinationTitle != page.redirect)
                           {
                             Logger.getLogger(Redirects.getClass.getName).log(Level.WARNING, "wrong redirect. page: [" + page.title + "].\nfound by dbpedia:   [" + destinationTitle + "].\nfound by wikipedia: [" + page.redirect + "]")
                           }

                           if (destinationTitle != null && page.title.namespace == Namespace.Template && destinationTitle.namespace == Namespace.Template)
                           {
                             List((page.title.decoded, destinationTitle.decoded))
                           }
                           else
                           {
                             Nil
                           }
                       }

    val redirects = new Redirects(redirectsRDD.collectAsMap().toMap)

    logger.info("Redirects loaded from source (" + lang.wikiCode + ")")
    redirects
  }

  private def buildRegex(lang: Language) =
  {
    val redirects = Redirect(lang).mkString("|")
    // (?ius) enables CASE_INSENSITIVE UNICODE_CASE DOTALL
    // case insensitive and unicode are important - that's what mediawiki does.
    // Note: Although we do not specify a Locale, UNICODE_CASE does mostly the right thing.
    // DOTALL means that '.' also matches line terminators.
    // Reminder: (?:...) are non-capturing groups, '*?' is a reluctant qualifier.
    // (?:#[^\n]*?)? is an optional (the last '?') non-capturing group meaning: there may
    // be a '#' after which everything but line breaks is allowed ('[]{}|<>' are not allowed
    // before the '#'). The match is reluctant ('*?'), which means that we recognize ']]'
    // as early as possible.
    // (?:\|[^\n]*?)? is another optional non-capturing group that reluctantly consumes
    // a '|' character and everything but line breaks after it.
    ("""(?ius)\s*(?:""" + redirects + """)\s*:?\s*\[\[([^\[\]{}|<>\n]+(?:#[^\n]*?)?)(?:\|[^\n]*?)?\]\].*""").r
  }
}

