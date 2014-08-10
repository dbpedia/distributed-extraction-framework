package org.dbpedia.extraction.dump.download

import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import java.io.File
import scala.io.{Codec, Source}
import org.dbpedia.extraction.util.HadoopConfigurable
import java.net.URL

/**
 * Distributed download and general download configuration
 * @param args List of arguments in key=value format as explained in the Usage object.
 */
class DistDownloadConfig(args: TraversableOnce[String]) extends HadoopConfigurable
{
  import DownloadConfig._

  private val downloadConfig = new DownloadConfig()

  downloadConfig.parse(null, args) // parse the general config file
  parse(null, args) // parse the distributed downloads config file
  if (baseDir == null) throw Usage("No target directory")
  if ((languages.nonEmpty || ranges.nonEmpty) && baseUrl == null) throw Usage("No base URL")
  if (languages.isEmpty && ranges.isEmpty) throw Usage("No files to download")
  if (!baseDir.exists && !baseDir.mkdirs()) throw Usage("Target directory '" + baseDir + "' does not exist and cannot be created")

  def wikiName = downloadConfig.wikiName

  def baseUrl = downloadConfig.baseUrl

  def dateRange = downloadConfig.dateRange

  def dumpCount = downloadConfig.dumpCount

  def retryMax = downloadConfig.retryMax

  def retryMillis = downloadConfig.retryMillis

  def unzip = downloadConfig.unzip

  def progressPretty = downloadConfig.progressPretty

  def languages = downloadConfig.languages

  def ranges = downloadConfig.ranges

  /**
   * Download directory. If absent in distributed download config, obtain it from the original config file.
   */
  var baseDir: Path = null

  /**
   * List of mirrors to download from. These will be comma-separated URLs (which are in the same format as baseUrl). Example:
   * mirrors=http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br,http://dumps.wikimedia.your.org/
   */
  var mirrors: Array[URL] = Array(new URL("http://dumps.wikimedia.org"))

  /**
   * If each language consists of multiple dump files (eg. enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2)
   * they are downloaded in parallel. Multiple languages are downloaded in parallel too, giving us 2 levels of parallelism.
   *
   * If sequentialLanguages is set to true, one language is downloaded at a time, otherwise, all languages are downloaded in parallel.
   */
  var sequentialLanguages: Boolean = false

  /**
   * Number of simultaneous downloads from each mirror per slave node. Set to 2 by default.
   */
  var threadsPerMirror = 2

  /** Path to hadoop core-site.xml, hadoop hdfs-site.xml and hadoop mapred-site.xml respectively */
  override protected val (hadoopCoreConf, hadoopHdfsConf, hadoopMapredConf) =
    parseHadoopConfigs(null, args)

  /**
   * @param dir Context directory. Config file and base dir names will be resolved relative to
   *            this path. If this method is called for a config file, this argument should be the directory
   *            of that file. Otherwise, this argument should be the current working directory (or null,
   *            which has the same effect).
   */
  def parse(dir: File, args: TraversableOnce[String])
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
        withSource(file)(source => parse(file.getParentFile, source.getLines()))
      case _ => //throw Usage("Invalid argument '" + arg + "'")
    }

    // First checks the Path obtained from distributed download config, then the general download config file if the former is null
    baseDir = checkPathExists(Option(
                                      new Path(if (distBaseDir != null)
                                               {
                                                 distBaseDir
                                               }
                                               else
                                               {
                                                 if (downloadConfig.baseDir == null) throw Usage("No target directory")
                                                 downloadConfig.baseDir.getPath
                                               })
                                    ), pathMustExist = true).get
  }

  /**
   * Parses the args to extract Hadoop *-site.xml configuration file paths
   * @return Tuple of Hadoop configuration file paths
   */
  private def parseHadoopConfigs(dir: File, args: TraversableOnce[String]): (String, String, String) =
  {
    var hadoopCoreConf: String = null
    var hadoopHdfsConf: String = null
    var hadoopMapredConf: String = null

    for (a <- args; arg = a.trim) arg match
    {
      case Ignored(_) => // ignore
      case Arg("hadoop-coresite-xml-path", file) => hadoopCoreConf = file
      case Arg("hadoop-hdfssite-xml-path", file) => hadoopHdfsConf = file
      case Arg("hadoop-mapredsite-xml-path", file) => hadoopMapredConf = file
      case Arg("distconfig", path) =>
        val file = resolveFile(dir, path)
        if (!file.isFile) throw Usage("Invalid file " + file, arg)
        withSource(file)
        {
          source =>
            val confs = parseHadoopConfigs(file.getParentFile, source.getLines())
            hadoopCoreConf = confs._1
            hadoopHdfsConf = confs._2
            hadoopMapredConf = confs._3
        }
      case _ => //throw Usage("Invalid argument '" + arg + "'")
    }
    (hadoopCoreConf, hadoopHdfsConf, hadoopMapredConf)
  }

  private def withSource(file: File)(func: Source => Unit)
  {
    val source = Source.fromFile(file)(Codec.UTF8)
    func(source)
    source.close()
  }
}

object Usage {
  def apply(msg: String, arg: String = null, cause: Throwable = null): Exception = {
    val message = if (arg == null) msg else msg+" in '"+arg+"'"

    println(message)
    val usage = /* empty line */ """
Usage (with example values):
==============================
General download configuration
==============================
config=/example/path/file.cfg
  Path to exisiting UTF-8 text file whose lines contain arguments in the format given here.
  Absolute or relative path. File paths in that config file will be interpreted relative to
  the config file.
base-dir=/example/path
  Path to existing target directory (on local file system or HDFS). Required.
download-dates=20120530-20120610
  Only dumps whose page date is in this range will be downloaded. By default, all dumps are
  included, starting with the newest. Open ranges like 20120530- or -20120610 are allowed.
download-count=1
  Max number of dumps to download. Default is 1.
download=en,zh-yue,1000-2000,...:file1,file2,...
  Download given files for given languages from server. Each key is either '@mappings', a language
  code, or a range. In the latter case, languages with a matching number of articles will be used.
  If the start of the range is omitted, 0 is used. If the end of the range is omitted,
  infinity is used. For each language, a new sub-directory is created in the target directory.
  Each file is a file name like 'pages-articles.xml.bz2' or a regex if it starts with a '@' (useful for
  multiple files processing, i.e. multiple parts of the same file) to which a prefix like
 'enwiki-20120307-' will be added. This argument can be used multiple times, for example
  'download=en:foo.xml download=de:bar.xml'. '@mappings' means all languages that have a
   mapping namespace on http://mappings.dbpedia.org.
retry-max=5
  Number of total attempts if the download of a file fails. Default is no retries.
retry-millis=1000
  Milliseconds between attempts if the download of a file fails. Default is 10000 ms = 10 seconds.
unzip=true
  Should downloaded .gz and .bz2 files be unzipped on the fly? Default is false.
pretty=true
  Should progress printer reuse one line? Doesn't work with log files, so default is false.
Order is relevant - for single-value parameters, values read later overwrite earlier values.
Empty arguments or arguments beginning with '#' are ignored.

==================================
Distributed download configuration
==================================
distconfig=/example/path/file.cfg
  Path to existing distributed download configuration text file (UTF-8) whose lines contain arguments
  in the format given here. Absolute or relative path. File paths in that config file will be interpreted
  relative to the config file.
mirrors=http://dumps.wikimedia.org/
  List of mirrors to download from. These will be comma-separated URLs.
  Example: mirrors=http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br,http://dumps.wikimedia.your.org/
threads-per-mirror=2
  Number of simultaneous downloads from each mirror per slave node. Set to 2 by default.
sequential-languages=false
  If each language consists of multiple dump files (eg. enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2)
  they are downloaded in parallel. Multiple languages are downloaded in parallel too, giving us 2 levels of
  parallelism. If sequentialLanguages is set to true, one language is downloaded at a time, otherwise,
  all languages are downloaded in parallel.
hadoop-coresite-xml-path=/path/to/core-site.xml
  Path to hadoop core-site.xml configuration file.
hadoop-hdfssite-xml-path=/path/to/hdfs-site.xml
  Path to hadoop hdfs-site.xml configuration file.
hadoop-mapredsite-xml-path=/path/to/mapred-site.xml
  Path to hadoop mapred-site.xml configuration file.
                                 """ /* empty line */
    println(usage)

    new Exception(message, cause)
  }
}