package org.dbpedia.extraction.dump.download.actors.message

object DownloaderProgressMessage
{
  // From Downloader or DownloadJobRunner to DownloadProgressTracker
  case class Read(bytesRead: Long)
  case class Start(totalBytes: Long) // totalBytes = total content length
  case object Stop

  // From DownloadProgressTracker to DownloadJobRunner
  case class ProgressEnd(bytes: Long)
}
