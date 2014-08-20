package org.dbpedia.extraction.util

import com.esotericsoftware.kryo.Kryo
import org.dbpedia.extraction.spark.serialize.KryoSerializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}

/**
 * Kryo file operations helper methods
 */
object DistIOUtils
{
  private val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo]
  {
    override def initialValue = getNewKryo()
  }

  /**
   * @return returns a thread-local instance of Kryo
   */
  def getKryoInstance: Kryo = kryo.get()

  /**
   * @return new Kryo instance.
   */
  def getNewKryo(): Kryo = KryoSerializer.ser.newKryo()

  /**
   * Loads an RDD saved as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param sc SparkContext
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   * @return deserialized RDD
   */
  def loadRDD[T: ClassTag](sc: SparkContext, rddClass: Class[T], path: String): RDD[T] =
  {
    val arrayOfRddClass = Class.forName("[L" + rddClass.getName + ";")
    val serializedRDD = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
    serializedRDD.values.flatMap(x => deserialize(x.getBytes, arrayOfRddClass).asInstanceOf[Array[T]])
  }

  /**
   * Loads an RDD saved as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param sc SparkContext
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   * @return deserialized RDD
   */
  def loadRDD[T: ClassTag](sc: SparkContext, rddClass: Class[T], path: Path): RDD[T] =
  {
    val arrayOfRddClass = Class.forName("[L" + rddClass.getName + ";")
    val conf = new Configuration()
    val job = Job.getInstance(conf)
    FileInputFormat.addInputPath(job, path)
    val updatedConf = job.getConfiguration
    val serializedRDD = sc.newAPIHadoopRDD(updatedConf, classOf[SequenceFileInputFormat[NullWritable, BytesWritable]], classOf[NullWritable], classOf[BytesWritable])
    serializedRDD.values.flatMap(x => deserialize(x.getBytes, arrayOfRddClass).asInstanceOf[Array[T]])
  }

  /**
   * Saves an RDD as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param rdd Spark RDD
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   */
  def saveRDD(rdd: RDD[_ <: AnyRef], path: String)
  {
    rdd.mapPartitions(iter => iter.grouped(50).map(_.toArray))
    .map(x => (NullWritable.get(), new BytesWritable(serialize(x)))).saveAsSequenceFile(path)
  }

  /**
   * Saves an RDD as a SequenceFile containing objects serialized by Kryo,
   * with NullWritable keys and BytesWritable values.
   * @param rdd Spark RDD
   * @param path String path to existing file. Can be on local file system or HDFS, S3 etc. See Spark docs.
   */
  def saveRDD(rdd: RDD[_ <: AnyRef], path: Path)
  {
    rdd.mapPartitions(iter => iter.grouped(50).map(_.toArray))
    .map(x => (NullWritable.get(), new BytesWritable(serialize(x)))).saveAsSequenceFile(path.toString)
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
    val stream = new ByteArrayOutputStream()
    val output = new Output(stream)
    getKryoInstance.writeObject(output, x)
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
    getKryoInstance.readObject(new Input(new ByteArrayInputStream(x)), c)
  }
}