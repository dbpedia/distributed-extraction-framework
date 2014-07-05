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
 * @param wikiName Config.wikiName (eg. wiki)
 * @param date Wiki dump date in YYYYMMDD format
 * @param outputSuffix Output suffix corresponding to formatter (eg. tql)
 * @param formatter Formatter object used to render the Quad objects according to a specific format
 */
class DBpediaDatasetOutputFormat(langWikiCode: String,
                                 wikiName: String,
                                 date: String,
                                 outputSuffix: String,
                                 formatter: Formatter) extends MultipleTextOutputFormat[Text, QuadSeqWritable]
{
  override protected def getBaseRecordWriter(out: DataOutputStream,
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

  override protected def generateFileNameForKeyValue(key: Text, value: QuadSeqWritable): String =
  {
    val datasetName = key.toString
    // eg. enwiki-20140614-article-categories.tql
    s"$langWikiCode$wikiName-$date-${datasetName.replace('_', '-')}.$outputSuffix"
  }

  /**
   * RecordWriter that wraps a LineRecordWriter, applies the given Formatter on a Seq[Quad] and writes to
   * the LineRecordWriter.
   */
  private class DBpediaDatasetRecordWriter(lineWriter: LineRecordWriter[NullWritable, Text]) extends RecordWriter[Text, QuadSeqWritable]
  {
    private val text = new Text("")

    /**
     * Note: using synchronization here is *probably* not strictly necessary, but without it, different sequences of quads
     * may be interleaved, which is harder to read and makes certain parsing optimizations impossible.
     */
    override def write(key: Text, value: QuadSeqWritable) =
      synchronized
      {
        for (quad <- value.get)
        {
          text.set(formatter.render(quad))
          lineWriter.write(NullWritable.get(), text)
        }
      }

    override def close(context: TaskAttemptContext) = synchronized(lineWriter.close(context))
  }

}
