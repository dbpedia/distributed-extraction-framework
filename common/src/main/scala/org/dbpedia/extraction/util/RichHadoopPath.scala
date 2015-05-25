package org.dbpedia.extraction.util

import java.io.{IOException, FileNotFoundException,OutputStream, InputStream}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.nio.file.NotDirectoryException
import scala.language.implicitConversions
//import org.dbpedia.extraction.util.FileLike

object RichHadoopPath {

  implicit def wrapPath(path: Path)(implicit hadoopConf: Configuration) = new RichHadoopPath(path, hadoopConf)

  implicit def toPath(path: String) = new Path(path)

}

/**
 * This class lets us use org.apache.hadoop.fs.Path seamlessly wherever a FileLike is used.
 * Defines additional methods on Path by using an implicit Configuration.
 */
class RichHadoopPath(path: Path, conf: Configuration) extends FileLike[Path] {

  private val fs: FileSystem = path.getFileSystem(conf)

  override def toString: String = path.toString

  override def name: String = path.getName

  /**
   * @throws NotDirectoryException if the path is not a directory
   * @throws FileNotFoundException if the path does not exist
   */
  override def hasFiles: Boolean = {
    isDirectory match {
      // Not a directory?
      case false => throw new NotDirectoryException(path.toString)
      // Contains files?
      case true => if(fs.listStatus(path).size > 0) true else false
    }
  }

  override def delete(recursive: Boolean = false): Unit = {
    if(!fs.delete(path, recursive))
      throw new IOException("failed to delete path ["+path+"]")
  }

  override def resolve(name: String): Path = new Path(path, name)

  override def exists: Boolean = fs.exists(path)

  // TODO: more efficient type than List?
  override def names: List[String] = names("*")

  // TODO: more efficient type than List?
  def names(glob: String): List[String] = list(glob).map(_.getName)

  // TODO: more efficient type than List?
  override def list: List[Path] = list("*")

  // TODO: more efficient type than List?
  def list(glob: String): List[Path] = {
    val list = fs.globStatus(new Path(path, glob)).map(_.getPath).toList
    if(list.isEmpty) throw new IOException("failed to list files in ["+path+"]")
    list
  }

  override def size: Long = fs.getContentSummary(path).getLength

  override def isFile: Boolean = fs.isFile(path)

  override def isDirectory: Boolean = fs.getFileStatus(path).isDirectory

  override def inputStream(): InputStream = fs.open(path)

  override def outputStream(append: Boolean = false): OutputStream = if(append) fs.append(path) else fs.create(path)

  def mkdirs(): Boolean = fs.mkdirs(path)

  def getSchemeWithFileName: String = fs.getScheme + "://" + path.toUri.getPath
}
