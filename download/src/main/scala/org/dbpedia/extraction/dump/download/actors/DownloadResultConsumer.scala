package org.dbpedia.extraction.dump.download.actors

import akka.actor.{ActorLogging, Actor}
import org.dbpedia.extraction.dump.download.actors.message.GeneralMessage.ShutdownCluster
import akka.contrib.pattern.{DistributedPubSubExtension, DistributedPubSubMediator}
import org.dbpedia.extraction.dump.download.actors.message.WorkerProgressMessage.{Progress, ProgressStart}
import org.dbpedia.extraction.dump.download.actors.message.{DownloadProgress, DownloadResult, DownloadJob, MirroredDownloadJob}

/**
 * This actor is used to print download progress logging messages on the driver/master node.
 * Hooks into Master.ResultsTopic and consumes DownloadResult messages.
 *
 * TODO: Refactor the code to pretty-print better progress results like ByteLogger. Maintain list of jobs
 * and log percentage of work done etc.
 */
class DownloadResultConsumer extends Actor with ActorLogging
{
  var jobs = Map[String, MirroredDownloadJob]()
  val mediator = DistributedPubSubExtension(context.system).mediator
  mediator ! DistributedPubSubMediator.Subscribe(Master.General, self)
  mediator ! DistributedPubSubMediator.Subscribe(Master.ProgressTopic, self)
  mediator ! DistributedPubSubMediator.Subscribe(Master.ResultsTopic, self)

  def receive =
  {
    case _: DistributedPubSubMediator.SubscribeAck =>

    case job @ MirroredDownloadJob(_, DownloadJob(downloadId, _)) =>
      log.info("Starting job: {}", job)
      jobs += (downloadId -> job)

    case DownloadResult(downloadId, outputPath, bytes) =>
      log.info("{}: {} bytes downloaded to {}", downloadId, bytes, outputPath)

    case DownloadProgress(downloadId, p @ ProgressStart(bytes)) =>
      log.info("{}: {}", jobs(downloadId), p)

    case DownloadProgress(downloadId, p @ Progress(bytes)) =>
      log.info("{}: {}", jobs(downloadId), p)

    case ShutdownCluster =>
      context.stop(self)
      context.system.shutdown()
  }
}
