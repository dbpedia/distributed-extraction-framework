package org.dbpedia.extraction.util

import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.conf.Configuration

/**
 * Trait for classes that need to create a Hadoop Configuration.
 */
trait HadoopConfigurable
{
  /** Path to hadoop core-site.xml */
  protected val hadoopCoreConf: String

  /** Path to hadoop hdfs-site.xml */
  protected val hadoopHdfsConf: String

  /** Path to hadoop mapred-site.xml */
  protected val hadoopMapredConf: String

  /** Hadoop Configuration. This is implicit because RichHadoopPath operations need it. */
  implicit lazy val hadoopConf =
  {
    val hadoopConf = new Configuration()

    if (hadoopCoreConf != null)
      hadoopConf.addResource(new Path(hadoopCoreConf))
    if (hadoopHdfsConf != null)
      hadoopConf.addResource(new Path(hadoopHdfsConf))
    if (hadoopMapredConf != null)
      hadoopConf.addResource(new Path(hadoopMapredConf))

    hadoopConf
  }

  /**
   * Checks if a Path exists.
   *
   * @param path Option[Path] if this is None, pathMustExist has no effect.
   * @param pathMustExist Boolean to ensure that the Path, if obtained, actually exists.
   * @throws RuntimeException if Option[Path] is defined but the path does not exist
   * @return the Option[Path] given as input
   */
  def checkPathExists(path: Option[Path], pathMustExist: Boolean): Option[Path] =
  {
    // If pathMustExist is set to true, and path is defined but it does not exist, throw an error.
    if (pathMustExist && path.isDefined && !path.get.exists)
    {
      val hadoopHint = if (hadoopCoreConf == null || hadoopHdfsConf == null || hadoopMapredConf == null) " Make sure you configured Hadoop correctly and the directory exists on the configured file system." else ""
      throw sys.error("Dir " + path.get.getSchemeWithFileName + " does not exist." + hadoopHint)
    }
    path
  }
}
