package org.dbpedia.extraction.spark.io.output

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat._
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordWriter}
import scala.collection.mutable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.LineRecordWriter
import java.io.DataOutputStream
import org.apache.hadoop.io.compress.{CompressionCodecFactory, CompressionCodec}

/**
 * This class extends allows writing output to multiple output files depending upon custom criteria. It filters
 * every key-value pair and routes them to the corresponding locations.
 *
 * Configuration variables:
 * dbpedia.output.overwrite - Boolean, if set to true, output files will be overwritten if they already exist,
 * or else an IOException will be thrown (which is also the default behaviour)
 */
class MultipleTextOutputFormat[K, V] extends TextOutputFormat[K, V]
{
  private val OVERWRITE = "dbpedia.output.overwrite"

  private class MultipleTextRecordWriter(context: TaskAttemptContext) extends RecordWriter[K, V]
  {
    private val recordWriters = mutable.Map[String, RecordWriter[K, V]]()

    /**
     * Note: This method is not synchronized, keeping with the rest of the Hadoop code in this framework.
     * When using this with Spark, set only one core per worker to ensure that only one thread accesses
     * this method per JVM.
     */
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
    val keyValueSeparator = conf.get(TextOutputFormat.SEPERATOR, "\t")
    // If overwriteOutput is set to true, output files will be overwritten if they already exist,
    // or else an IOException will be thrown (which is also the default behaviour)
    val overwriteOutput = conf.getBoolean(OVERWRITE, false)

    val (codec, file) = if (inferCodecFromPathName)
    {
      val extension = pathName.substring(pathName.lastIndexOf('.'))
      // Get modified suffixed path
      val file = getModifiedWorkFile(pathName, context, extension)
      // Returns Option[CompressionCodec] or None depending on file extension
      val codec = Option(new CompressionCodecFactory(conf).getCodec(file))
      (codec, file)
    }
    else
    {
      val isCompressed = getCompressOutput(context)
      if (isCompressed)
      {
        // Get the CompressionCodec from job configuration
        val codecClass = getOutputCompressorClass(context, classOf[CompressionCodec])
        val codec = ReflectionUtils.newInstance(codecClass, conf)
        val file = getModifiedWorkFile(pathName, context, codec.getDefaultExtension)
        (Some(codec), file)
      }
      else
      {
        val file = getModifiedWorkFile(pathName, context, "")
        (None, file)
      }
    }

    val fs = file.getFileSystem(conf)
    val fileOutputStream = fs.create(file, overwriteOutput)

    getBaseRecordWriter(context, fileOutputStream, keyValueSeparator, codec)
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
   * If inferCodecFromPathName is set to true, the output compression codec will be inferred from the suffix/extension
   * in pathName (eg. foobar.gz implies GzipCodec is used), otherwise it uses Hadoop configuration settings.
   *
   * The default behaviour is to use Hadoop configuration settings.
   */
  protected val inferCodecFromPathName: Boolean = false

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
  protected def getBaseRecordWriter(context: TaskAttemptContext,
                                    out: DataOutputStream,
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
