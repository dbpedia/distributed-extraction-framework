package org.dbpedia.extraction.spark.io

import org.dbpedia.extraction.destinations.Quad
import org.apache.hadoop.io.Writable
import org.dbpedia.extraction.util.DistIOUtils
import java.io.{DataInputStream, DataOutput, ByteArrayOutputStream, DataInput}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.EnumerationSerializer

/**
 * Created by nilesh on 4/7/14.
 */
class QuadSeqWritable(quads: Seq[Quad])  extends Writable
{
  var _quads = quads

  def this() = this(null)

  def set(quads: Seq[Quad])
  {
    _quads = quads
  }

  def get = _quads

  override def write(output: DataOutput)
  {
    val out = new ByteArrayOutputStream()
    val o = new Output(out, 10)
    DistIOUtils.getKryoInstance.writeClassAndObject(o, _quads)
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
    _quads = DistIOUtils.getKryoInstance.readClassAndObject(i).asInstanceOf[Seq[Quad]]
  }
}
