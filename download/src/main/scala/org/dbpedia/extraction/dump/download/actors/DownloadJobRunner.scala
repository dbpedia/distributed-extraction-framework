package org.dbpedia.extraction.dump.download.actors

import akka.actor.{Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import org.dbpedia.extraction.dump.download.{Unzip, ActoredCounter, FileDownloader}
import org.dbpedia.extraction.util.Finder
import java.net.URL
import org.dbpedia.extraction.util.RichHadoopPath.wrapPath
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import org.dbpedia.extraction.dump.download.actors.Worker.DownloadComplete
import org.dbpedia.extraction.dump.download.actors.protocol.DownloaderProgressProtocol
import DownloaderProgressProtocol.{ProgressEnd, Stop}

/**
 * This actor is used by Worker to run a download job.
 *
 * @param progressInterval Download progress report interval
 * @param hadoopConfiguration Hadoop Configuration
 * @param tempDir temporary directory on local file system to download to (before being moved to HDFS)
 * @param unzip true if file should be unzipped while downloading, false otherwise
 */
class DownloadJobRunner(progressInterval: FiniteDuration, hadoopConfiguration: Configuration, tempDir: File, unzip: Boolean) extends Actor
{
  implicit private val _hadoopConfiguration = hadoopConfiguration
  implicit private val progressStopTimeout = Timeout(5 seconds)

  val progress = context.watch(context.actorOf(Props(classOf[DownloadProgressTracker], context.parent, progressInterval), "progress"))

  class Downloader extends FileDownloader with ActoredCounter
  {
    override val progressActor = progress
  }

  val downloader =
    if (unzip) new Downloader with Unzip
    else new Downloader

  def receive =
  {
    case MirroredDownloadJob(mirror, DownloadJob(_, DumpFile(baseDir, wikiName, lang, date, fileName))) =>
      val s = sender()
      import context.dispatcher

      val finder = new Finder[Path](baseDir, lang, wikiName)
      val wiki = finder.wikiName
      val dateDir = baseDir.resolve(wiki).resolve(date)
      if (!dateDir.exists && !dateDir.mkdirs) throw new Exception("Target directory [" + dateDir.getSchemeWithFileName + "] does not exist and cannot be created")
      if (!tempDir.exists && !tempDir.mkdirs) throw new Exception("Local temporary directory [" + tempDir + "] does not exist and cannot be created")

      val url = new URL(mirror, s"$wiki/$date/$wiki-$date-$fileName")
      println(url.toString)
      println(dateDir)
      println(dateDir.getSchemeWithFileName)

      Future(downloader.downloadTo(url, tempDir)).
      onSuccess
      {
        case file =>
          val fs = dateDir.getFileSystem(hadoopConfiguration)
          val outputPath = dateDir.resolve(file.getName)
          fs.moveFromLocalFile(new Path(file.toURI), outputPath)
          progress ? Stop onSuccess
            {
              case ProgressEnd(totalBytes) =>
                s ! DownloadComplete(outputPath.getSchemeWithFileName, totalBytes)
            }
      }
  }
}

object DownloadJobRunner
{
  def props(progressInterval: FiniteDuration, hadoopConfiguration: Configuration, tempDir: File, unzip: Boolean = false): Props =
    Props(classOf[DownloadJobRunner], progressInterval, hadoopConfiguration, tempDir, unzip)
}
