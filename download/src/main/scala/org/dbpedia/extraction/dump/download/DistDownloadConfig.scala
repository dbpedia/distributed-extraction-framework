package org.dbpedia.extraction.dump.download

import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import java.io.File
import scala.io.{Codec, Source}
import org.dbpedia.extraction.util.HadoopConfigurable
import java.net.URL
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{AddressFromURIString, Address}
import scala.collection.mutable.ListBuffer

/**
 * Distributed download and general download configuration
 * @param args List of arguments in key=value format as explained in the Usage object.
 */
class DistDownloadConfig(args: TraversableOnce[String]) extends HadoopConfigurable
{

  import DownloadConfig._

  private val downloadConfig = new DownloadConfig()
  private val generalArgs = new ListBuffer[String]()

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
  var mirrors: Array[URL] = Array(new URL("http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br/,http://ftp.fi.muni.cz/pub/wikimedia/,http://dumps.wikimedia.your.org/"))

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
  var threadsPerMirror: Int = 2

  /**
   * Number of workers to run per slave. Set to 2 by default.
   */
  var workersPerSlave: Int = 2

  /**
   * Progress report time interval - the driver node receives real-time progress reports for running downloads
   * from the workers. If a worker fails to send a progress report of the current download under the given timeout
   * (the timeout is usually set to something like progressReportInterval + 2 to be safe) the download job will be marked
   * as failed and inserted back into the pending download queue.
   *
   * This is 2 seconds by default.
   */
  var progressReportInterval: FiniteDuration = 2 seconds

  /**
   * Maximum number of consecutive duplicate progress read bytes to tolerate. The workers keep track of download progress;
   * if a download gets stuck consecutive progress reports will contain the same number of bytes downloaded. If this is set
   * to 30 (not recommended to go below that), the worker will declare a job as failed only after getting the same progress
   * report for 30 times.
   */
  var maxDuplicateProgress: Int = 30

  /**
   * Local temporary directory on worker nodes. Each dump file/chunk is downloaded to this directory before being moved to
   * the configured Hadoop file system.
   */
  var localTempDir: File = new File("/dump/")

  /**
   * Slave hostnames. By default consists only of 127.0.0.1.
   */
  var slaves: Array[String] = Array("127.0.0.1")

  /**
   * Master host. By default set to 127.0.0.1.
   */
  var master: String = "127.0.0.1"

  /**
   * Akka join address on the driver node. This is where the workers connect to.
   * This must be defined when starting up a worker.
   */
  var joinAddress: Option[Address] = None

  /**
   * Current logged in user name, also the user name used to connect to cluster nodes.
   */
  var userName: String = System.getProperty("user.name")

  /**
   * Optional identity file to connect to cluster nodes via SSH.
   */
  var privateKey: Option[String] = None

  /**
   * Optional passphrase for SSH private key.
   */
  var sshPassphrase: Option[String] = None

  /**
   * Absolute path to the distributed extraction framework (containing this module) in all nodes
   */
  var homeDir: String = null

  def isMaster: Boolean = !joinAddress.isDefined

  /** Path to hadoop core-site.xml, hadoop hdfs-site.xml and hadoop mapred-site.xml respectively */
  override protected val (hadoopCoreConf, hadoopHdfsConf, hadoopMapredConf) =
    parseHadoopConfigs(null, args)


  parse(null, args) // parse the distributed download config file/variables

  if (homeDir == null)
    throw Usage("Config variable extraction-framework-home not specified!")

  println("parse dist-config file done")

  downloadConfig.parse(null, generalArgs.toList) // parse the general config file

  println("config files parsed")
  if ((languages.nonEmpty || ranges.nonEmpty) && baseUrl == null) throw Usage("No base URL")
  if (languages.isEmpty && ranges.isEmpty) throw Usage("No files to download")

  println("base url and files to download found")
  // First checks the Path obtained from distributed download config, then the general download config file if the former is null
  baseDir = checkPathExists(Option(
                                   if (baseDir != null)
                                   {
                                     baseDir
                                   }
                                   else
                                   {
                                     new Path({
                                                if (downloadConfig.baseDir == null) throw Usage("No target directory")
                                                downloadConfig.baseDir.getPath
                                              })
                                   }
                                  ), pathMustExist = true
                           ).get

  if (!baseDir.exists && !baseDir.mkdirs())
    throw Usage("Target directory '" + baseDir.getSchemeWithFileName + "' does not exist and cannot be created")

  /**
   * @param dir Context directory. Config file and base dir names will be resolved relative to
   *            this path. If this method is called for a config file, this argument should be the directory
   *            of that file. Otherwise, this argument should be the current working directory (or null,
   *            which has the same effect).
   */
  def parse(dir: File, args: TraversableOnce[String])
  {
    // Parse the distributed config variables and accumulate the remaining variables in the generalArgs list.

    for (a <- args; arg = a.trim) arg match
    {
      case Ignored(_) => // ignore comments
      case Arg("mirrors", urls) => mirrors = urls.split(",").map(url => toURL(if (url endsWith "/") url else url + "/", arg)) // must have slash at end
      case Arg("base-dir", path) => baseDir = new Path(path)
      case Arg("threads-per-mirror", threads) => threadsPerMirror = toInt(threads, 1, Int.MaxValue, arg)
      case Arg("workers-per-slave", workers) => workersPerSlave = toInt(workers, 1, Int.MaxValue, arg)
      case Arg("sequential-languages", bool) => sequentialLanguages = toBoolean(bool, arg)
      case Arg("progress-interval", interval) => progressReportInterval = toInt(interval, 1, Int.MaxValue, arg).seconds
      case Arg("max-duplicate-progress-reports", max) => maxDuplicateProgress = toInt(max, 1, Int.MaxValue, arg)
      case Arg("local-temp-dir", file) => localTempDir = new File(file)
      case Arg("master", host) => master = host
      case Arg("slaves", hosts) => slaves = hosts.split(",")
      case Arg("extraction-framework-home", path) => homeDir = path
      case Arg("join", uri) => joinAddress = Some(AddressFromURIString(uri))
      case Arg("private-key", file) => privateKey = Some(file)
      case Arg("ssh-passphrase", pass) => sshPassphrase = Some(pass)
      case Arg("distconfig", path) =>
        val file = resolveFile(dir, path)
        if (!file.isFile) throw Usage("Invalid file " + file, arg)
        withSource(file)(source => parse(file.getParentFile, source.getLines()))
      case other if !(other.startsWith("hadoop-coresite-xml-path")
        || other.startsWith("hadoop-hdfssite-xml-path")
        || other.startsWith("hadoop-mapredsite-xml-path")) => generalArgs += other
      case _ => // leave the hadoop-* stuff for parseHadoopConfigs
    }
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

object exUsage
{
  def apply(msg: String, arg: String = null, cause: Throwable = null): Exception =
  {
    val message = if (arg == null) msg else msg + " in '" + arg + "'"

    println(message)
    val usage = /* empty line */ """
Usage (with example values):
==============================
General download configuration
==============================
config=/example/path/file.cfg
  Path to exisiting UTF-8 text file whose lines contain arguments in the format given here.
  Absolute or relative path. File paths in that config file will be interpreted relative to the
  config file. Note that this file must be present in the same path in all nodes of the cluster.
base-url=http://dumps.wikimedia.org/
  Base URL of dump server/mirror to use when building the list of dump files/URLs. Required.
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
extraction-framework-home=/path/to/distributed-extraction-framework
  This must be set to the absolute path to the distributed extraction framework (containing this module)
  in all nodes. No default value is set.
mirrors=http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br/,http://ftp.fi.muni.cz/pub/wikimedia/,http://dumps.wikimedia.your.org/
  List of mirrors to download from in the form of comma-separated URLs. Choose from the list of mirrors at:
  http://meta.wikimedia.org/wiki/Mirroring_Wikimedia_project_XML_dumps#Current_Mirrors
  Example: mirrors=http://dumps.wikimedia.org/,http://wikipedia.c3sl.ufpr.br,http://ftp.fi.muni.cz/pub/wikimedia/,http://dumps.wikimedia.your.org/
threads-per-mirror=2
  Number of simultaneous downloads from each mirror per slave node. Set to 2 by default.
workers-per-slave=2
  Number of workers to run per slave. This is set to 2 by default.
  Setting it to (no. of mirrors) * threads-per-mirror is recommended for exploiting maximum parallelism. On the other hand,
  if your whole cluster has only one public facing IP it is better to set this to a low number like 1.
progress-interval=2
  Progress report time interval in secs - the driver node receives real-time progress reports for running downloads from the workers.
  If a worker fails to send a progress report of the current download under the given timeout (the timeout is set to something
  like progressReportInterval + 2 to be safe) the download job will be marked as failed and inserted back into the pending
  download queue. This is 2 seconds by default.
max-duplicate-progress-reports=30
  Maximum number of consecutive duplicate progress read bytes to tolerate. The workers keep track of download progress;
  if a download gets stuck consecutive progress reports will contain the same number of bytes downloaded. If this is set
  to 30 (not recommended to go below that), the worker will declare a job as failed only after getting the same progress
  report for 30 times. By default set to 30.
local-temp-dir=/tmp
  Local temporary directory on worker nodes. Each dump file/chunk is downloaded to this directory before being moved to
  the configured Hadoop file system. This is /tmp by default.
private-key=/path/to/id_rsa
  Optional identity file to connect to cluster nodes via SSH.
ssh-passphrase=passphrase
  Optional passphrase for SSH private key.
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
master=127.0.0.1
  Master node host.
slaves=127.0.0.1
  List of comma-separated slave hosts. Example: slaves=node1,node2,node3
join=akka.tcp://Workers@hostname:port
  This variable needs to be specified when starting up a worker manually. Do not use this variable unless you know what you're
  doing. The driver node automatically starts up workers on the slaves and takes care of this variable. Never set this variable
  when starting up the master/driver.
                                 """ /* empty line */
    println(usage)

    new Exception(message, cause)
  }
}