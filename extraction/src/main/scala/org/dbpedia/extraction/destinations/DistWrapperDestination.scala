package org.dbpedia.extraction.destinations

import org.apache.spark.rdd.RDD

/**
 * Base class for DistDestination objects that forward most calls to another destination.
 */
abstract class DistWrapperDestination(destination: DistDestination) extends DistDestination
{
  override def open() = destination.open()

  def write(rdd: RDD[Seq[Quad]]) = destination.write(rdd)

  override def close() = destination.close()
}