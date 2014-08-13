package org.dbpedia.extraction.dump.download.actors

import akka.actor.{ActorLogging, Actor}
import org.dbpedia.extraction.dump.download.actors.protocol.GeneralProtocol.ShutdownCluster
import akka.contrib.pattern.{DistributedPubSubExtension, DistributedPubSubMediator}

/**
 * This actor is used to print download progress logging messages on the driver/master node.
 * Hooks into Master.ResultsTopic and consumes DownloadResult messages.
 */
class DownloadResultConsumer extends Actor with ActorLogging
{
  val mediator = DistributedPubSubExtension(context.system).mediator
  mediator ! DistributedPubSubMediator.Subscribe(Master.General, self)
  mediator ! DistributedPubSubMediator.Subscribe(Master.ResultsTopic, self)

  def receive =
  {
    case _: DistributedPubSubMediator.SubscribeAck =>
    case DownloadResult(downloadId, outputPath, bytes) =>
      log.info("{}: {} bytes downloaded to {}", downloadId, bytes, outputPath)
    case ShutdownCluster =>
      context.stop(self)
      context.system.shutdown()
  }
}
