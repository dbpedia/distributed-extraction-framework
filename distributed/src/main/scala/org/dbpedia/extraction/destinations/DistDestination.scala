package org.dbpedia.extraction.destinations

import org.apache.spark.rdd.RDD

/**
 * A distributed destination for RDF quads.
 */
trait DistDestination extends Destination
{
  /**
   * Writes RDD of quads to this destination.
   *
   * @param rdd RDD[ Seq[Quad] ]
   */
  def write(rdd: RDD[Seq[Quad]]): Unit

  /**
   * This is not implemented.
   *
   * @param graph Traversable[Quad]
   */
  final override def write(graph: Traversable[Quad]): Unit = ???
}
