package org.dbpedia.extraction.spark.io


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.dbpedia.extraction.destinations.Quad
import scala.util.Random
import org.junit.Assert._
import org.scalatest.FunSuiteLike

@RunWith(classOf[JUnitRunner])
class QuadSeqWritableTest extends WritableTest[QuadSeqWritable] with FunSuiteLike
{
  test("Verify that serialization-deserialization works properly")
  {
    // Create random List[Quad]
    val sampleQuads = for (i <- (0 until 100).toList) yield new Quad(Random.nextString(10),
      Random.nextString(10),
      Random.nextString(10),
      Random.nextString(10),
      Random.nextString(10),
      Random.nextString(10),
      Random.nextString(10))

    val writable1 = new QuadSeqWritable(sampleQuads)
    val writable2 = new QuadSeqWritable()

    performReadWriteRoundTrip(writable1, writable2)
    assertEquals(writable1.get.toString, writable2.get.toString)

  }
}
