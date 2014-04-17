package org.dbpedia.extraction.mappings

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.sources.WikiPage
import collection.mutable.{HashSet, HashMap}
import java.io._
import util.control.ControlThrowable
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.wikiparser.impl.wikipedia.Redirect
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.dbpedia.extraction.spark.serialize.KryoSerializationWrapper

/**
 * Holds the redirects between wiki pages
 * At the moment, only redirects between Templates are considered
 *
 * This class uses Spark to compute the redirects.
 *
 * @param map Redirect map. Contains decoded template titles.
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
   */
  def load(rdd: RDD[WikiPage], cache: File, lang: Language): Redirects =
  {
    //Try to load redirects from the cache
    try
    {
      return loadFromCache(cache)
    }
    catch
      {
        case ex: Exception => logger.log(Level.INFO, "Will extract redirects from source for " + lang.wikiCode + " wiki, could not load cache file '" + cache + "': " + ex)
      }

    //Load redirects from RDD
    val redirects = loadFromRDD(rdd, lang)

    val dir = cache.getParentFile
    if (!dir.exists && !dir.mkdirs) throw new IOException("cache dir [" + dir + "] does not exist and cannot be created")
    val outputStream = new ObjectOutputStream(new FileOutputStream(cache))
    try
    {
      outputStream.writeObject(redirects.map)
    }
    finally
    {
      outputStream.close()
    }
    logger.info(redirects.map.size + " redirects written to cache file " + cache)

    redirects
  }

  /**
   * Loads the redirects from a cache file.
   */
  private def loadFromCache(cache: File): Redirects =
  {
    logger.info("Loading redirects from cache file " + cache)
    val inputStream = new ObjectInputStream(new FileInputStream(cache))
    try
    {
      val redirects = new Redirects(inputStream.readObject().asInstanceOf[Map[String, String]])

      logger.info(redirects.map.size + " redirects loaded from cache file " + cache)
      redirects
    }
    finally
    {
      inputStream.close()
    }
  }

  /**
   * Loads the redirects from a source.
   */
  def loadFromRDD(rdd: RDD[WikiPage], lang: Language): Redirects =
  {
    logger.info("Loading redirects from source (" + lang.wikiCode + ")")

    val redirectFinder = new RedirectFinder(lang)

    // TODO: usually, flatMap can be applied to Option, but not here. That's why
    // RedirectFinder.apply returns a List, not an Option. Some implicit missing?
    def genMapper(kryoWrapper: KryoSerializationWrapper[(WikiPage => List[(String, String)])])
                 (page: WikiPage): List[(String, String)] =
    {
      kryoWrapper.value.apply(page)
    }
    val mapper = genMapper(KryoSerializationWrapper(new RedirectFinder(lang))) _
    val redirects = new Redirects(rdd.flatMap(mapper).collectAsMap().toMap)

    logger.info("Redirects loaded from source (" + lang.wikiCode + ")")
    redirects
  }

  private class RedirectFinder(lang: Language) extends (WikiPage => List[(String, String)])
  {
    val regex = buildRegex

    private def buildRegex =
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

    override def apply(page: WikiPage): List[(String, String)] =
    {
      val destinationTitle: WikiTitle =
        page.source match
        {
          case regex(destination) =>
          {
            try
            {

              WikiTitle.parse(destination, page.title.language)
            }
            catch
              {
                case ex: WikiParserException =>
                {
                  Logger.getLogger(Redirects.getClass.getName).log(Level.WARNING, "Couldn't parse redirect destination", ex)
                  null
                }
              }
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
  }

}