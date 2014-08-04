package org.dbpedia.extraction.spark.io

import org.apache.hadoop.io.Writable
import java.io.{ByteArrayOutputStream, DataOutput, DataInput}
import org.dbpedia.extraction.sources.WikiPage
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.spark.serialize.WikiPageSerializer
import org.dbpedia.extraction.util.DistIOUtils

/**
 * DBpediaWikiPageInputFormat emits values of type WikiPageWritable. This class holds a single WikiPage instance.
 * @see DBpediaWikiPageInputFormat
 */
class WikiPageWritable(wikiPage: WikiPage) extends Writable
{
  var _wikiPage = wikiPage

  def this() = this(null)

  def set(wikiPage: WikiPage)
  {
    _wikiPage = wikiPage
  }

  def get = _wikiPage

  val wps = new WikiPageSerializer

  override def write(output: DataOutput)
  {
    val out = new ByteArrayOutputStream()
    val o = new Output(out)
    wps.write(DistIOUtils.getKryoInstance, o, get)
    o.close()
    val bytes = out.toByteArray
    output.writeInt(bytes.size)
    output.write(bytes)
  }

  override def readFields(input: DataInput)
  {
    val size = input.readInt()
    val bytes = new Array[Byte](size)
    input.readFully(bytes)
    val i = new Input()
    i.setBuffer(bytes)
    set(wps.read(DistIOUtils.getKryoInstance, i, classOf[WikiPage]))
    i.close()
  }
}