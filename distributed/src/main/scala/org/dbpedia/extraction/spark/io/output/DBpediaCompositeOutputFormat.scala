package org.dbpedia.extraction.spark.io.output

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.dbpedia.extraction.spark.io.QuadSeqWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import scala.collection.{Map, mutable}
import org.dbpedia.extraction.destinations.formatters.{UriPolicy, Formatter}
import org.dbpedia.extraction.util.SparkUtils
import java.util.Properties

/**
 * OutputFormat implementation that uses the configured Formatters to write Quads to respective datasets
 * through the DBpediaDatasetOutputFormat class. This class uses as many DBpediaDatasetOutputFormat objects
 * as there are configured formats. Formats are read in from the provided extraction config properties file.
 * This class handles configuration and Formatters, while DBpediaDatasetOutputFormat handles dividing the Quads
 * into datasets.
 *
 * To use this OutputFormat four Strings need to be set in Hadoop's Configuration:
 *
 * dbpedia.config.properties - The extraction config Properties object serialized to String
 * dbpedia.wiki.name - Config.wikiName (eg. wiki)
 * dbpedia.wiki.language.wikicode - Language wiki code of the input wiki dump
 * dbpedia.wiki.date - Wiki dump date in YYYYMMDD format
 *
 * Also, the output needs to be grouped by dataset such that each key is a Text representing the dataset to which
 * the Quads in the value belong to. Example key: article_categories
 *
 * Output will look like Hadoop leaf files (eg. part-00000) inside directories like enwiki-20140614-article-categories.tql.
 * The files will be compressed using the specified compression codec.
 *
 * @see DBpediaDatasetOutputFormat
 * @see org.dbpedia.extraction.destinations.DBpediaDatasets
 */
class DBpediaCompositeOutputFormat extends TextOutputFormat[Text, QuadSeqWritable]
{
  private val CONFIG = "dbpedia.config.properties"
  private val WIKI = "dbpedia.wiki.name"
  private val LANGUAGE = "dbpedia.wiki.language.wikicode"
  private val DATE = "dbpedia.wiki.date"

  private class DBpediaCompositeRecordWriter(context: TaskAttemptContext) extends RecordWriter[Text, QuadSeqWritable]
  {
    private val recordWriters = mutable.Map[String, RecordWriter[Text, QuadSeqWritable]]()
    private val conf = context.getConfiguration
    private val wikiName = conf.get(WIKI)
    private val langCode = conf.get(LANGUAGE)
    private val date = conf.get(DATE)
    private val formatters: Map[String, Formatter] =
    {
      // Deserialize the config Properties object to get the Formatters
      val config = SparkUtils.getObjectFromConfiguration(CONFIG, conf).asInstanceOf[Properties]
      UriPolicy.parseFormats(config, "uri-policy", "format")
    }

    override def write(key: Text, value: QuadSeqWritable)
    {
      for ((suffix, format) <- formatters)
      {
        // Each RecordReader writes Quads to corresponding datasets depending upon the Text key.
        // See DBpediaDatasetOutputFormat and MultipleTextOutputFormat for details.
        val writer = recordWriters.getOrElseUpdate(suffix, new DBpediaDatasetOutputFormat(
                                                                                           langCode,
                                                                                           wikiName,
                                                                                           date,
                                                                                           suffix,
                                                                                           format
                                                                                         ).getRecordWriter(context))
        writer.write(key, value)
      }
    }

    override def close(context: TaskAttemptContext) = recordWriters.foreach(_._2.close(context))
  }

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Text, QuadSeqWritable] = new DBpediaCompositeRecordWriter(context)
}
