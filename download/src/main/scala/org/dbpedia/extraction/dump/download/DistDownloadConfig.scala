package org.dbpedia.extraction.dump.download

import org.apache.hadoop.fs.Path
import java.io.File
import scala.io.{Codec, Source}
import org.dbpedia.extraction.util.HadoopConfigurable
import java.net.{MalformedURLException, URL}

/**
 * Created by nilesh on 6/8/14.
 */
class DistDownloadConfig(configFile: File, distConfigFile: File) extends HadoopConfigurable
{
  /**
   * Parse the original download configuration file and distributed download configuration file.
   * Each line in file must be an argument as explained by usage overview.
   */
  downloadConfig.parse(configFile) // parse the original config file
  withSource(distConfigFile)(source => parse(distConfigFile.getParentFile, source.getLines()))

  val downloadConfig = new DownloadConfig()

  var wikiName = downloadConfig.wikiName
  var baseUrl = downloadConfig.baseUrl
  var dateRange = downloadConfig.dateRange
  var dumpCount = downloadConfig.dumpCount
  var retryMax = downloadConfig.retryMax
  var retryMillis = downloadConfig.retryMillis
  var unzip = downloadConfig.unzip
  var progressPretty = downloadConfig.progressPretty
  val languages = downloadConfig.languages
  val ranges = downloadConfig.ranges

  /**
   * Download directory. If absent in distributed download config, obtain it from the original config file.
   */
  var baseDir: Path = null

  /**
   * List of mirrors to download from. These will be comma-separated URLs (which are in the same format as baseUrl). Example:
   * mirrors=http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br,http://dumps.wikimedia.your.org/
   */
  var mirrors = Array("http://dumps.wikimedia.org")

  /**
   * If each language consists of multiple dump files (eg. enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2)
   * they are downloaded in parallel. Multiple languages are downloaded in parallel too, giving us 2 levels of parallelism.
   *
   * If sequentialLanguages is set to true, one language is downloaded at a time, otherwise, all languages are downloaded in parallel.
   */
  var sequentialLanguages: Boolean = false

  /**
   * Number of simultaneous downloads from each mirror per slave node.
   */
  var threadsPerMirror = 2

  /** Path to hadoop core-site.xml, hadoop hdfs-site.xml and hadoop mapred-site.xml respectively */
  override protected val (hadoopCoreConf, hadoopHdfsConf, hadoopMapredConf) =
  {
    var hadoopCoreConf: String = null
    var hadoopHdfsConf: String = null
    var hadoopMapredConf: String = null

    withSource(distConfigFile)
    {
      source =>
        val dir = distConfigFile.getParentFile
        val args = source.getLines()
        for (a <- args; arg = a.trim) arg match
        {
          case Ignored(_) => // ignore
          case Arg("hadoop-coresite-xml-path", file) => hadoopCoreConf = file
          case Arg("hadoop-hdfssite-xml-path", file) => hadoopHdfsConf = file
          case Arg("hadoop-mapredsite-xml-path", file) => hadoopMapredConf = file
          case _ => throw Usage("Invalid argument '" + arg + "'")
        }
    }

    (hadoopCoreConf, hadoopHdfsConf, hadoopMapredConf)
  }

  //  /**
  //   * Parse the original download configuration file and distributed download configuration file.
  //   * Each line in file must be an argument as explained by usage overview.
  //   */
  //  def parse(configFile: File, distConfigFile: File): Unit =
  //  {
  //    downloadConfig.parse(configFile) // parse the original config file
  //    withSource(distConfigFile)(source => parse(distConfigFile.getParentFile, source.getLines()))
  //  }

  /**
   * @param dir Context directory. Config file and base dir names will be resolved relative to
   *            this path. If this method is called for a config file, this argument should be the directory
   *            of that file. Otherwise, this argument should be the current working directory (or null,
   *            which has the same effect).
   */
  def parse(dir: File, args: TraversableOnce[String]): Unit =
  {
    var distBaseDir: String = null

    for (a <- args; arg = a.trim) arg match
    {
      case Ignored(_) => // ignore
      case Arg("mirrors", urls) => mirrors = urls.split(",").map(url => toURL(if (url endsWith "/") url else url + "/", arg)) // must have slash at end
      case Arg("base-dir", path) => distBaseDir = path
      case Arg("threads-per-mirror", threads) => threadsPerMirror = toInt(threads, 1, Int.MaxValue, arg)
      case Arg("sequential-languages", bool) => sequentialLanguages = toBoolean(bool, arg)
      case Arg("distconfig", path) =>
        val file = resolveFile(dir, path)
        if (!file.isFile) throw Usage("Invalid file " + file, arg)
        parse(file)
      case _ => throw Usage("Invalid argument '" + arg + "'")
    }

    baseDir = getPath(new Path(downloadConfig.baseDir.getPath), distBaseDir)
  }

  /**
   * First checks the Path obtained from distributed download config, then the general download config file..
   *
   * @param originalBaseDir base-dir from general download config file
   * @param distBaseDir base-dir from distributed download config file
   * @throws RuntimeException if the obtained path does not exist
   * @return the obtained Path
   */
  private def getPath(originalBaseDir: Path, distBaseDir: Path) =
  {
    val somePath = Option(if (distBaseDir != null) distBaseDir else originalBaseDir)
    checkPathExists(somePath, pathMustExist = true)
    somePath.get
  }

  // TODO: Make resolveFile, toBoolean, toInt and toURL public in DownloadConfig and reuse them instead of copying code?

  /**
   * If path is absolute, return it as a File. Otherwise, resolve it against parent.
   * (This method does what the File(File, String) constructor should do. Like URL(URL, String))
   * @param parent may be null
   * @param path must not be null, may be empty
   */
  private def resolveFile(parent: File, path: String): File = {
    val child = new File(path)
    val file = if (child.isAbsolute) child else new File(parent, path)
    // canonicalFile removes '/../' etc.
    file.getCanonicalFile
  }

  private def toBoolean(s: String, arg: String): Boolean =
    if (s == "true" || s == "false") s.toBoolean else throw Usage("Invalid boolean value", arg)

  private def toInt(str: String, min: Int, max: Int, arg: String): Int =
    try
    {
      val result = str.toInt
      if (result < min) throw new NumberFormatException(str + " < " + min)
      if (result > max) throw new NumberFormatException(str + " > " + max)
      result
    }
    catch
      {
        case nfe: NumberFormatException => throw Usage("invalid integer", arg, nfe)
      }

  private def toURL(s: String, arg: String): URL =
    try new URL(s)
    catch
      {
        case mue: MalformedURLException => throw Usage("Invalid URL", arg, mue)
      }

  private def withSource(file: File)(func: Source => Unit)
  {
    val source = Source.fromFile(file)(Codec.UTF8)
    func(source)
    source.close()
  }
}
