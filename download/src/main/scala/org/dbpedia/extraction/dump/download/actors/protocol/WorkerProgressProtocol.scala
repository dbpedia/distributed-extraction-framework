package org.dbpedia.extraction.dump.download.actors.protocol

object WorkerProgressProtocol
{
  // DownloadProgressTracker to Worker
  case class Progress(bytes: Long)
  case class ProgressStart(bytes: Long)
}
