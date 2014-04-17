package org.dbpedia.extraction.util

import com.esotericsoftware.kryo.Kryo
import java.util.Locale
import org.dbpedia.extraction.spark.serialize.LocaleSerializer
import org.objenesis.strategy.StdInstantiatorStrategy
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag

/**
 * Kryo file operations helper methods
 */
object DistIOUtils
{
  /**
   * @return new Kryo instance.
   */
  def getNewKryo(): Kryo =
  {
    val kryo = new Kryo()
    kryo.addDefaultSerializer(classOf[Locale], classOf[LocaleSerializer])
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy())
    kryo
  }

  /**
   * Loads an RDD saved as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param sc SparkContext
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   * @return deserialized RDD
   */
  def loadRDD[T: ClassTag](sc: SparkContext, rddClass: Class[T], path: String): RDD[T] =
  {
    val serializedRDD = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
    serializedRDD.values.map(x => rddClass.cast(deserialize(x.getBytes, rddClass)))
  }

  /**
   * Saves an RDD as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param rdd Spark RDD
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   */
  def saveRDD(rdd: RDD[_ <: AnyRef], path: String)
  {
    rdd.map(x => (NullWritable.get(), new BytesWritable(serialize(x)))).saveAsSequenceFile(path)
  }

  //  TODO: Add unit tests with code similar to:
  //  /**
  //   * Temporary method to test if serialization-deserialization works properly
  //   */
  //  def testSerDe(rdd: RDD[_ <: AnyRef], path: String) {
  //    val serialized = rdd.map(x => (NullWritable.get(), new BytesWritable(serialize(x))))
  //    serialized.saveAsSequenceFile(path)
  //
  //    val deserialized : RDD[_ <: AnyRef] = serialized.values.map(x => {
  //      deserialize(x.getBytes, classOf[WikiPage]).asInstanceOf[WikiPage]
  //    })
  //
  //    //Assertions below to test if (de)serialization works properly.
  //    assert(deserialized.first().toString == rdd.first().toString)
  //    assert(deserialized.count() == rdd.count())
  //  }
  //
  //  /**
  //   * Temporary method to test if saveAsKryoFile() and openFromKryoFile() work consistently.
  //   */
  //  def testSaveOpen(sc: SparkContext, rdd: RDD[_ <: WikiPage], path: String) {
  //    saveRDD(rdd, path)
  //    val deserialized = loadRDD(sc, path)
  //
  //    //Test to ensure we're saving as many WikiPages as we're retrieving after deserialization
  //    assert(deserialized.count() == rdd.count())
  //  }

  /**
   * @param x Any object
   * @return serialized Array of Bytes
   */
  def serialize(x: Any): Array[Byte] =
  {
    val kryo = getNewKryo()
    val stream = new ByteArrayOutputStream()
    val output = new Output(stream)
    kryo.writeObject(output, x)
    output.close()
    stream.toByteArray
  }

  /**
   * @param x Array of Bytes - serialized version of an object
   * @param c Class of the object
   * @return the object deserialized by Kryo
   */
  def deserialize[T](x: Array[Byte], c: Class[T]) =
  {
    val kryo = getNewKryo()
    kryo.readObject(new Input(new ByteArrayInputStream(x)), c)
  }
}