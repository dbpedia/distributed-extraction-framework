package org.dbpedia.extraction.destinations

import org.apache.spark.rdd.RDD

/**
 * A distributed destination for RDF quads.
 */
trait DistDestination
{
  /**
   * Opens this destination. This method should only be called once during the lifetime
   * of a destination, and it should not be called concurrently with other methods of this class.
   */
  def open(): Unit

  /**
   * Writes RDD of quads to this destination.
   *
   * @param rdd RDD[ Seq[Quad] ]
   */
  def write(rdd: RDD[Seq[Quad]]): Unit

  /**
   * Closes this destination. This method should only be called once during the lifetime
   * of a destination, and it should not be called concurrently with other methods of this class.
   */
  def close(): Unit
}
