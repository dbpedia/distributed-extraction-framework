package org.dbpedia.extraction.dump.download

import akka.actor.ActorRef
import java.io.InputStream
import java.net.URLConnection
import org.dbpedia.extraction.util.CountingInputStream
import org.dbpedia.extraction.dump.download.actors.message.DownloaderProgressMessage
import DownloaderProgressMessage.{Start, Read}
import Counter.getContentLength

/**
 * A Downloader mixin to be used with DownloadProgressTracker. Sends Start/Read messages to
 * the DownloadProgressTracker actor reference.
 *
 * @see org.dbpedia.extraction.dump.download.actors.DownloadProgressTracker
 */
trait ActoredCounter extends Downloader
{
  /**
   * Reference to a DownloadProgressTracker actor.
   */
  val progressActor: ActorRef

  protected abstract override def inputStream(conn: URLConnection): InputStream = {
    def logger(bytesRead: Long, close: Boolean): Unit = progressActor ! Read(bytesRead)
    progressActor ! Start(getContentLength(conn)) // Signal start of download with the total file size in bytes
    new CountingInputStream(super.inputStream(conn), logger)
  }

}
