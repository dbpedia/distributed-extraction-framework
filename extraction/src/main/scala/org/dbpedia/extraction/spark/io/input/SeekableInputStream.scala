package org.dbpedia.extraction.spark.io.input

import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.{FileSystem, Seekable, FSDataInputStream}
import java.io.{InputStream, FilterInputStream}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

object SeekableInputStream
{
  /**
   * Examines a FileSplit and returns the appropriate SeekableInputStream generated from it.
   *
   * @param split FileSplit to generate the SeekableInputStream from
   * @param fs FileSystem
   * @param compressionCodecs CompressionCodecFactory
   * @return SeekableInputStream to read from split
   */
  def apply(split: FileSplit, fs: FileSystem, compressionCodecs: CompressionCodecFactory): SeekableInputStream =
  {
    val path = split.getPath
    val start = split.getStart
    val end = start + split.getLength

    val codec = compressionCodecs.getCodec(path)
    val dataInputStream = fs.open(path)

    codec match
    {
      case splitableCodec: SplittableCompressionCodec =>
        // Is it a splittable compression input stream?
        val compressionInputStream = splitableCodec.createInputStream(dataInputStream,
                                                                      CodecPool.getDecompressor(codec),
                                                                      start,
                                                                      end,
                                                                      SplittableCompressionCodec.READ_MODE.BYBLOCK)
        SeekableSplitCompressedInputStream(compressionInputStream)
      case null =>
        // Input stream not compressed?
        dataInputStream.seek(start)
        SeekableUncompressedInputStream(dataInputStream)
      case _ =>
        // Non-splittable compression input stream? No seeking or offsetting is needed
        assert(start == 0)
        val compressionInputStream = codec.createInputStream(dataInputStream, CodecPool.getDecompressor(codec))
        SeekableCompressedInputStream(compressionInputStream, dataInputStream)
    }
  }
}

/**
* A SeekableInputStream internally using a SplitCompressionInputStream, ie. compressed by a splittable compression method.
*/
case class SeekableSplitCompressedInputStream(sin: SplitCompressionInputStream) extends SeekableInputStream(sin, sin)

/**
* A compressed SeekableInputStream using a non-splittable compression input stream
*/
case class SeekableCompressedInputStream(cin: CompressionInputStream, fsin: FSDataInputStream) extends SeekableInputStream(cin, fsin)

/**
* SeekableInputStream without compression.
*/
case class SeekableUncompressedInputStream(fsin: FSDataInputStream) extends SeekableInputStream(fsin, fsin)

/**
* Wraps an InputStream and a corresponding Seekable to track its position.
*
* @param in InputStream to read binary data from
* @param seeker Seekable for the InputStream "in" - used for keeping track of position in the InputStream
*/
sealed class SeekableInputStream(in: InputStream, seeker: Seekable) extends FilterInputStream(in) with Seekable
{
  override def getPos: Long = seeker.getPos

  override def seek(pos: Long) = seeker.seek(pos)

  override def seekToNewSource(targetPos: Long): Boolean = seeker.seekToNewSource(targetPos)

  override def toString: String = in.toString
}