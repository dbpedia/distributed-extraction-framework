package org.dbpedia.extraction.mappings

import java.util.logging.{Level, Logger}
import java.io._
import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.dbpedia.extraction.util.{DistIOUtils, Language}
import org.apache.hadoop.conf.Configuration
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * A version of Disambiguations that works with org.apache.hadoop.fs.Path.
 *
 * @see Disambiguations
 */
class DistDisambiguations(override val set : Set[Long]) extends Disambiguations(set)

object DistDisambiguations
{
  private val logger = Logger.getLogger(classOf[DistDisambiguations].getName)

  /** Implicit Configuration needed for RichHadoopPath operations */
  private implicit var hadoopConfImpl: Configuration = null

  /**
   * Loads disambiguations from cache/source reader.
   *
   * @param reader Reader to load disambiguations from
   * @param cache Path to cache file
   * @param lang Language
   * @param hadoopConf Configuration
   * @return Disambiguations object
   */
  def load(reader : () => Reader, cache : Path, lang : Language, hadoopConf: Configuration) : Disambiguations =
  {
    this.hadoopConfImpl = hadoopConf

    try
    {
      return loadFromCache(cache)
    }
    catch
      {
        case ex : Exception => logger.log(Level.INFO, "Will extract disambiguations from source for "+lang.wikiCode+" wiki, could not load cache file '"+cache+"': "+ex)
      }

    val disambiguations = Disambiguations.loadFromFile(reader, lang)

    val dir = cache.getParent
    val fs = dir.getFileSystem(hadoopConf)
    if (!dir.exists && !fs.mkdirs(dir)) throw new IOException("cache dir [" + dir + "] does not exist and cannot be created")
    val output = new Output(new BufferedOutputStream(cache.outputStream()))

    try
    {
      DistIOUtils.getKryoInstance.writeClassAndObject(output, disambiguations.set)
      logger.info(disambiguations.set.size + " disambiguations written to cache file " + cache)
      disambiguations
    }
    finally
    {
      output.close()
    }
  }

  /**
   * Loads the disambiguations from a cache file.
   */
  private def loadFromCache(cache : Path) : Disambiguations =
  {
    logger.info("Loading disambiguations from cache file " + cache)
    val input = new Input(new BufferedInputStream(cache.inputStream()))
    try
    {
      val disambiguations = new Disambiguations(DistIOUtils.getKryoInstance.readClassAndObject(input).asInstanceOf[Set[Long]])
      logger.info(disambiguations.set.size + " disambiguations loaded from cache file " + cache)
      disambiguations
    }
    finally
    {
      input.close()
    }
  }
}
