package org.dbpedia.extraction.dump.download.actors.message

object WorkerProgressMessage
{
  // DownloadProgressTracker to Worker
  trait ProgressMessage
  case class Progress(bytes: Long) extends ProgressMessage
  case class ProgressStart(bytes: Long) extends ProgressMessage
}
