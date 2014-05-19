package org.dbpedia.extraction.spark.serialize

import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}


/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer
{

  @transient lazy val ser: SparkKryoSerializer =
  {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkKryoSerializer(sparkConf)
  }

  def serialize[T](o: T): Array[Byte] =
  {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T =
  {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}