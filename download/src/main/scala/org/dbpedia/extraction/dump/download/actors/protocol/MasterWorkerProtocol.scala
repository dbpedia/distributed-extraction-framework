package org.dbpedia.extraction.dump.download.actors.protocol

object MasterWorkerProtocol
{
  // Messages from Workers
  case class RegisterWorker(workerId: String)
  case class WorkerRequestsDownload(workerId: String)
  case class DownloadIsDone(workerId: String, downloadId: String, outputPath: String, bytes: Long)
  case class DownloadFailed(workerId: String, downloadId: String)
  case class ProgressReport(workerId: String, progress: Long) // progress = number of bytes read till now
  case class RemoveWorker(workerId: String)

  // Messages to Workers
  case object DownloadIsReady
  case class Ack(id: String)
}
