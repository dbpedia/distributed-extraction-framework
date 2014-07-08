package org.dbpedia.extraction.destinations

import org.dbpedia.extraction.util.FileLike
import org.apache.spark.rdd.RDD

/**
 * Distributed version of MakerDestination that mixes in the DistDestination trait.
 *
 * Handles a marker file that signals that the extraction is either running ('start mode')
 * or finished ('end mode').
 *
 * In 'start mode', the file is created before the extraction starts (it must not already exist)
 * and deleted after the extraction ends.
 *
 * In 'end mode', the file is deleted before the extraction starts (if it already exists)
 * and re-created after the extraction ends.
 *
 * @param file marker file
 * @param start 'start mode' if true, 'end mode' if false.
 */
class DistMarkerDestination(destination: DistDestination, file: FileLike[_], start: Boolean)
  extends MarkerDestination(destination, file, start) with DistDestination
{
  override def write(rdd: RDD[Seq[Quad]]) = destination.write(rdd)
}