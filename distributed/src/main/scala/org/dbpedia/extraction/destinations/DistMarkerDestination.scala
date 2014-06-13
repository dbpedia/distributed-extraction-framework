package org.dbpedia.extraction.destinations

import org.apache.hadoop.fs.{FileSystem, Path}
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.conf.Configuration
import java.io.IOException

/**
 * A version of MarkerDestination that works with org.apache.hadoop.fs.Path.
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
 * @param path Hadoop Path to marker file
 * @param start 'start mode' if true, 'end mode' if false.
 *
 * @see MarkerDestination
 */
class DistMarkerDestination(destination: Destination, path: Path, start: Boolean, implicit val hadoopConf: Configuration)
extends WrapperDestination(destination)
{
  private val fs: FileSystem = path.getFileSystem(hadoopConf)

  override def open(): Unit = {
    if (start) create() else delete()
    super.open()
  }

  override def close(): Unit = {
    super.close()
    if (!start) create() else delete()
  }
  
  private def create(): Unit = {
    if (fs.exists(path)) throw new IOException("file '"+path+"' already exists")
    path.outputStream().close()
  }
  
  private def delete(): Unit = {
    if (!fs.exists(path)) return
    if (!path.deleteConfirm()) throw new IOException("failed to delete file '"+path+"'")
  }
  
}
