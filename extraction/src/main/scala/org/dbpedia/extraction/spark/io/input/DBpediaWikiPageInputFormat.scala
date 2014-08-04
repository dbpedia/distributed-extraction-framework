package org.dbpedia.extraction.spark.io.input

import org.apache.hadoop.io.{DataOutputBuffer, LongWritable}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.fs.Path
import scala.xml.XML
import org.dbpedia.extraction.sources.XMLSource
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.mapreduce.{JobContext, RecordReader, InputSplit, TaskAttemptContext}
import org.apache.commons.logging.LogFactory
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.spark.io.WikiPageWritable

/**
* Hadoop InputFormat that splits a Wikipedia dump file into WikiPageWritable (representing a single
* org.dbpedia.extraction.sources.WikiPage) chunks.
*
* The WikiPageRecordReader class inside outputs a WikiPageWritable as value and the starting position (byte) as key.
*
* Note that wikipage.language.wikicode needs to be set in Hadoop's Configuration.
*/
class DBpediaWikiPageInputFormat extends FileInputFormat[LongWritable, WikiPageWritable]
{
  private val LOG = LogFactory.getLog(classOf[DBpediaWikiPageInputFormat])
  private val LANGUAGE = "dbpedia.wiki.language.wikicode"

  protected override def isSplitable(context: JobContext, file: Path): Boolean =
  {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(file)
    if (null == codec) true else codec.isInstanceOf[SplittableCompressionCodec]
  }

  override def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, WikiPageWritable] =
  {
    val split = genericSplit.asInstanceOf[FileSplit]
    LOG.info("getRecordReader start.....split=" + split)
    context.setStatus(split.toString)
    new WikiPageRecordReader(split, context)
  }

  private class WikiPageRecordReader(split: FileSplit, context: TaskAttemptContext) extends RecordReader[LongWritable, WikiPageWritable]
  {
    private var key: LongWritable = null
    private var value: WikiPageWritable = null

    private val conf = context.getConfiguration

    // Language code for this data dump
    private val language = Language(conf.get(LANGUAGE))
    private val page = new DataOutputBuffer()
    private val inputStream = SeekableInputStream(split,
                                                  split.getPath.getFileSystem(conf),
                                                  new CompressionCodecFactory(conf))
    private val matcher = new ByteMatcher(inputStream)

    private val (start, end) =
    {
      inputStream match
      {
        case SeekableSplitCompressedInputStream(sin) =>
          (sin.getAdjustedStart, sin.getAdjustedEnd + 1)
        case _ =>
          (split.getStart, split.getStart + split.getLength)
      }
    }

    private val pageBeginPattern = "<page>".getBytes("UTF-8")
    private val pageEndPattern = "</page>".getBytes("UTF-8")

    override def close() = inputStream.close()

    override def getProgress: Float =
    {
      if (end == start) 1.0f else (getPos - start).asInstanceOf[Float] / (end - start).asInstanceOf[Float]
    }

    def getPos: Long = matcher.getPos

    override def initialize(genericInputSplit: InputSplit, context: TaskAttemptContext) = ()

    override def nextKeyValue(): Boolean =
    {
      // Initialize key and value
      if (key == null) key = new LongWritable()
      if (value == null) value = new WikiPageWritable()

      if (matcher.getPos < end && matcher.readUntilMatch(pageBeginPattern, end))
      {
        try
        {
          page.write(pageBeginPattern)
          if (matcher.readUntilMatch(pageEndPattern, end, Some(page)))
          {
            // Key is set to the position (bytes) where the page is found
            key.set(matcher.getPos)

            // Set value to the WikiPage created from the parsed <page>...</page>
            val elem = XML.loadString("<mediawiki>" + new String(page.getData.take(page.getLength), "UTF-8") + "</mediawiki>")
            value.set(XMLSource.fromXML(elem, language).head)

            return true
          }
        }
        finally
        {
          page.reset()
        }
      }
      false
    }

    override def getCurrentKey: LongWritable = key

    override def getCurrentValue: WikiPageWritable = value
  }

}
