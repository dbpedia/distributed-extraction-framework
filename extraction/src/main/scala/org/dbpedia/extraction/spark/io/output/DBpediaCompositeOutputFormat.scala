package org.dbpedia.extraction.spark.io.output

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.dbpedia.extraction.spark.io.QuadSeqWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}
import scala.collection.mutable
import org.dbpedia.extraction.destinations.formatters.UriPolicy
import org.dbpedia.extraction.util.ConfigUtils
import org.apache.commons.io.FilenameUtils
import java.io.File
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * OutputFormat implementation that uses the configured Formatters to write Quads to respective datasets
 * through the DBpediaDatasetOutputFormat class. This class uses as many DBpediaDatasetOutputFormat objects
 * as there are configured formats. Formats are read in from the provided extraction config properties file.
 * This class handles configuration and Formatters, while DBpediaDatasetOutputFormat handles dividing the Quads
 * into datasets.
 *
 * 1. To use this OutputFormat three Strings need to be set in Hadoop's Configuration:
 * dbpedia.wiki.name - Config.wikiName, the wiki suffix (eg. wiki)
 * dbpedia.wiki.language.wikicode - Language wiki code of the input wiki dump
 * dbpedia.wiki.date - Wiki dump date in YYYYMMDD format
 * dbpedia.output.overwrite - Boolean, if set to true, output files will be overwritten if they already exist,
 * or else an IOException will be thrown (which is also the default behaviour) - this is actually for MultipleTextOutputFormat
 * dbpedia.config.properties - HDFS Path at which the extraction config properties file is stored
 *
 * 2. The extraction config properties file needs to be added to the distributed cache - the HDFS location should be
 * configured using dbpedia.config.properties.
 *
 * 3. Also, the output needs to be grouped by dataset such that each key is a Text representing the dataset
 * to which the Quads in the value belong to. Example key: article_categories
 *
 * NOTE: When using this with Spark set only one core per worker.
 *
 * Output will look like Hadoop leaf files (eg. part-r-00000) inside directories like enwiki-20140614-article-categories.tql.
 * The files will be compressed using the specified compression codec.
 *
 * @see DBpediaDatasetOutputFormat
 */
class DBpediaCompositeOutputFormat extends TextOutputFormat[Text, QuadSeqWritable]
{
  private val CONFIG_PROPERTIES = "dbpedia.config.properties"
  private val WIKI = "dbpedia.wiki.name"
  private val LANGUAGE = "dbpedia.wiki.language.wikicode"
  private val DATE = "dbpedia.wiki.date"

  private class DBpediaCompositeRecordWriter(context: TaskAttemptContext) extends RecordWriter[Text, QuadSeqWritable]
  {
    private val recordWriters = mutable.Map[String, RecordWriter[Text, QuadSeqWritable]]()
    private val conf = context.getConfiguration
    private val configPropertiesDCPath = conf.get(CONFIG_PROPERTIES)
    private val wikiName = conf.get(WIKI)
    private val langCode = conf.get(LANGUAGE)
    private val date = conf.get(DATE)
    private val localConfigPropertiesFile = new Path("./config.properties")
    private val formatters =
    {
      // Deserialize the config Properties object to get the Formatters
      println(context.getCacheFiles.mkString("\n"))
      val configProperties = context.getCacheFiles.find(_.getPath == configPropertiesDCPath).get

      val fs = FileSystem.get(conf)
      // copy config file from distributed cache to raw local FS
      fs.copyToLocalFile(false, new Path(configProperties), localConfigPropertiesFile, true)

      val config = ConfigUtils.loadConfig(localConfigPropertiesFile.toString, "UTF-8")
      UriPolicy.parseFormats(config, "uri-policy", "format")
    }

    /**
     * Note: This method is not synchronized, keeping with the rest of the Hadoop code in this framework.
     * When using this with Spark set only one core per worker to ensure that only one thread accesses
     * this method per JVM.
     */
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

  override def checkOutputSpecs(job: JobContext) = () // allow overwriting output directory
}
