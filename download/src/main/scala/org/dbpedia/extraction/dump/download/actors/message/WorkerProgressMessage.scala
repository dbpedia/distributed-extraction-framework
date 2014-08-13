package org.dbpedia.extraction.dump.download.actors.message

object WorkerProgressMessage
{
  // DownloadProgressTracker to Worker
  case class Progress(bytes: Long)
  case class ProgressStart(bytes: Long)
}
