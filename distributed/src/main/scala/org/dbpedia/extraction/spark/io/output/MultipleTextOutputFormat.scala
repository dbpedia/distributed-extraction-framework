package org.dbpedia.extraction.spark.io.output

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat._
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}
import scala.collection.mutable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.LineRecordWriter
import java.io.DataOutputStream
import org.apache.hadoop.io.compress.CompressionCodec

/**
 * This class extends allows writing output to multiple output files depending upon custom criteria. It filters
 * every key-value pair and routes them to the corresponding locations.
 */
class MultipleTextOutputFormat[K, V] extends TextOutputFormat[K, V]
{

  private class MultipleTextRecordWriter(context: TaskAttemptContext) extends RecordWriter[K, V]
  {
    private val recordWriters = mutable.Map[String, RecordWriter[K, V]]()

    override def write(key: K, value: V)
    {
      // Generate the path depending upon key-value pair
      val finalPath = generateFileNameForKeyValue(key, value)

      // Extract the actual key and value
      val actualKey = generateActualKey(key, value)
      val actualValue = generateActualValue(key, value)

      // Get the RecordReader for finalPath or create one if needed
      val writer = recordWriters.getOrElseUpdate(finalPath, createRecordWriter(finalPath, context))
      writer.write(actualKey, actualValue)
    }

    override def close(context: TaskAttemptContext) = recordWriters.foreach(_._2.close(context))
  }

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = new MultipleTextRecordWriter(context)

  /**
   * Create a new RecordWriter based on the modified output path and the RecordWriter implementation
   * returned by getBaseRecordWriter().
   */
  private def createRecordWriter(pathName: String, context: TaskAttemptContext): RecordWriter[K, V] =
  {
    val conf = context.getConfiguration
    val isCompressed = getCompressOutput(context)
    val keyValueSeparator = conf.get(TextOutputFormat.SEPERATOR, "\t")

    val (codec, extension) = if (isCompressed)
    {
      val codecClass = getOutputCompressorClass(context, classOf[Nothing])
      val codec = ReflectionUtils.newInstance(codecClass, conf)
      (Some(codec), codec.getDefaultExtension)
    }
    else
    {
      (None, "")
    }

    val file = getModifiedWorkFile(pathName, context, extension)
    val fs = file.getFileSystem(conf)
    val fileOutputStream = fs.create(file, false)

    getBaseRecordWriter(fileOutputStream, keyValueSeparator, codec)
  }

  /**
   * Gets the default output path and inserts directoryName between the parent directory and leaf file (part-*).
   */
  private def getModifiedWorkFile(directoryName: String,
                                  context: TaskAttemptContext,
                                  extension: String): Path =
  {
    val path = super.getDefaultWorkFile(context, extension)
    new Path(new Path(path.getParent, directoryName), path.getName)
  }

  /**
   * Construct the underlying RecordWriter. By default creates a LineRecordWriter that is used by
   * TextOutputFormat by default.
   *
   * @param out DataOutputStream where output data is written to
   * @param keyValueSeparator String separator between output key and value
   * @param codec Option[CompressionCodec] for handling compression
   * @return A RecordWriter object over the given DataOutputStream
   */
  protected def getBaseRecordWriter(out: DataOutputStream,
                                    keyValueSeparator: String,
                                    codec: Option[CompressionCodec] = None): RecordWriter[K, V] =
  {
    codec match
    {
      case Some(c) =>
        // Have we an output compression codec?
        new LineRecordWriter[K, V](
                                    new DataOutputStream(c.createOutputStream(out)),
                                    keyValueSeparator
                                  )
      case _ =>
        new LineRecordWriter[K, V](out, keyValueSeparator)
    }
  }

  /**
   * Generate the output file name (the directory where the leaf part-* files will be written to)
   * based on the given key and value. The default behavior is that the file name does not depend on them.
   * That is, by default this method returns an empty String.
   *
   * @param key the key of the output data
   * @return generated file name
   */
  protected def generateFileNameForKeyValue(key: K, value: V): String = ""

  /**
   * Generate the actual key from the given key/value. The default behavior is that
   * the actual key is equal to the given key.
   *
   * @param key the key of the output data
   * @param value the value of the output data
   * @return the actual key derived from the given key/value
   */
  protected def generateActualKey(key: K, value: V): K = key

  /**
   * Generate the actual value from the given key and value. The default behavior is that
   * the actual value is equal to the given value.
   *
   * @param key the key of the output data
   * @param value the value of the output data
   * @return the actual value derived from the given key/value
   */
  protected def generateActualValue(key: K, value: V): V = value
}
