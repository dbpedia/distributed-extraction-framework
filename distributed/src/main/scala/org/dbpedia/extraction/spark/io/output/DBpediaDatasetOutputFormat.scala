package org.dbpedia.extraction.spark.io.output

import org.apache.hadoop.io.{Text, NullWritable}
import org.dbpedia.extraction.destinations.formatters.Formatter
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.LineRecordWriter
import org.dbpedia.extraction.spark.io.QuadSeqWritable
import java.io.DataOutputStream
import org.apache.hadoop.io.compress.CompressionCodec

/**
 * OutputFormat implementation that writes Quads to respective datasets depending upon the key, after applying
 * a given Formatter. This class extends MultipleTextOutputFormat which allows it to write to multiple locations
 * (for multiple datasets) depending upon custom criteria.
 *
 * The output needs to be grouped by dataset such that each key is a Text representing the dataset to which
 * the Quads in the value belong to. Example key: article_categories
 *
 * @param langWikiCode Language wiki code of the input wiki dump
 * @param wikiNameSuffix Config.wikiName (eg. wiki)
 * @param date Wiki dump date in YYYYMMDD format
 * @param outputSuffix Output suffix corresponding to formatter (eg. tql)
 * @param formatter Formatter object used to render the Quad objects according to a specific format
 */
class DBpediaDatasetOutputFormat(langWikiCode: String,
                                 wikiNameSuffix: String,
                                 date: String,
                                 outputSuffix: String,
                                 formatter: Formatter) extends MultipleTextOutputFormat[Text, QuadSeqWritable]
{
  /**
   * Construct the underlying RecordWriter. By default creates a LineRecordWriter that is used by
   * TextOutputFormat by default.
   *
   * @param context TaskAttemptContext
   * @param out DataOutputStream where output data is written to
   * @param keyValueSeparator String separator between output key and value
   * @param codec Option[CompressionCodec] for handling compression
   * @return A RecordWriter object over the given DataOutputStream
   */
  override protected def getBaseRecordWriter(context: TaskAttemptContext,
                                             out: DataOutputStream,
                                             keyValueSeparator: String,
                                             codec: Option[CompressionCodec] = None): RecordWriter[Text, QuadSeqWritable] =
  {
    // Get a LineRecordWriter (the usual RecordWriter used by TextOutputFormat) that ignores keys and writes Text outputs.
    val lineWriter = codec match
    {
      case Some(c) =>
        // Have we an output compression codec?
        new LineRecordWriter[NullWritable, Text](
                                                  new DataOutputStream(c.createOutputStream(out)),
                                                  keyValueSeparator
                                                )
      case _ =>
        new LineRecordWriter[NullWritable, Text](out, keyValueSeparator)
    }

    new DBpediaDatasetRecordWriter(lineWriter)
  }

  /**
   * If inferCodecFromPathName is set to true, the output compression codec will be inferred from the suffix/extension
   * in pathName (eg. tql.gz implies GzipCodec is used), otherwise it uses Hadoop configuration settings.
   */
  override protected val inferCodecFromPathName = true

  /**
   * Generate the output file name (the directory where the leaf part-* files will be written to)
   * based on the given key and value. The default behavior is that the file name does not depend on them.
   * That is, by default this method returns an empty String.
   *
   * @param key the key of the output data
   * @return generated file name
   */
  override protected def generateFileNameForKeyValue(key: Text, value: QuadSeqWritable): String =
  {
    val datasetName = key.toString
    // eg. enwiki-20140614-article-categories.tql
    s"$langWikiCode$wikiNameSuffix-$date-${datasetName.replace('_', '-')}.$outputSuffix"
  }

  /**
   * RecordWriter that wraps a LineRecordWriter, applies the given Formatter on a Seq[Quad] and writes to
   * the LineRecordWriter.
   */
  private class DBpediaDatasetRecordWriter(lineWriter: LineRecordWriter[NullWritable, Text]) extends RecordWriter[Text, QuadSeqWritable]
  {
    private val text = new Text("")
    private val nullKey = NullWritable.get()

    // Begin writing split with formatter header
    text.set(formatter.header.dropRight(1)) // remove newline from header
    lineWriter.write(nullKey, text)

    /**
     * Note: This method is not synchronized, keeping with the rest of the Hadoop code in this framework.
     * When using this with Spark, set only one core per worker to ensure that only one thread accesses
     * this method per JVM.
     */
    override def write(key: Text, value: QuadSeqWritable) =
    {
      for (quad <- value.get)
      {
        text.set(formatter.render(quad).dropRight(1)) // remove newline from rendered output
        lineWriter.write(nullKey, text)
      }
    }

    override def close(context: TaskAttemptContext) =
    {
      text.set(formatter.footer.dropRight(1)) // remove newline from footer
      lineWriter.write(nullKey, text)
      lineWriter.close(context)
    }
  }

}
