package org.dbpedia.extraction.spark.io

import org.apache.hadoop.io.Writable
import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream}

abstract class WritableTest[T <: Writable]
{
  /**
   * Utility method that takes two Writables as parameters, writes the first Writable to a byte
   * array and reads it back into the second Writable.
   *
   * @param oldWritable Writable to be serialized and deserialized again
   * @param newWritable Writable where oldWritable is deserialized into after serialization.
   */
  def performReadWriteRoundTrip(oldWritable: T, newWritable: T) =
  {
    val bos = new ByteArrayOutputStream
    val dos = new DataOutputStream(bos)
    oldWritable.write(dos)
    newWritable.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
  }
}
