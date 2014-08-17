package org.dbpedia.extraction.dump.download.actors

import akka.actor.{ActorLogging, Actor}
import java.util.UUID
import scala.concurrent.duration._
import akka.pattern._
import akka.contrib.pattern.{DistributedPubSubMediator, DistributedPubSubExtension}
import akka.contrib.pattern.DistributedPubSubMediator.Send
import org.dbpedia.extraction.dump.download.actors.message.GeneralMessage.{MasterQueueEmpty, ShutdownCluster}
import akka.util.Timeout
import org.dbpedia.extraction.dump.download.actors.message.{DumpFile, DownloadJob}

/**
 * A client actor used to submit download jobs to the master. To submit a job, a DumpFile object is sent as message.
 */
class DownloadClient extends Actor with ActorLogging
{

  import DownloadClient._
  import context.dispatcher

  def scheduler = context.system.scheduler

  def nextDownloadId(): String = UUID.randomUUID().toString

  val mediator = DistributedPubSubExtension(context.system).mediator
  mediator ! DistributedPubSubMediator.Subscribe(Master.General, self)

  implicit val timeout = Timeout(10.seconds)

  var canShutDownCluster = false

  def receive =
  {
    case Finished =>
      // send this when no more DumpFiles are to be added - ready for shutdown
      canShutDownCluster = true

    case MasterQueueEmpty =>
      if (canShutDownCluster) self ! ShutdownCluster

    case ShutdownCluster =>
      mediator ! Send("/user/master/active", ShutdownCluster, localAffinity = false)
      context.stop(self)
      context.system.shutdown()
      context.become(shuttingDown)

    case file: DumpFile =>
      self ! DownloadJob(nextDownloadId(), file)

    case job: DownloadJob =>
      (mediator ? Send("/user/master/active", job, localAffinity = false)) map
        {
          case Master.Ack(_) =>
            log.info("Job accepted by master: {}", job)
        } recover
        {
          case _ =>
            log.info("Job not accepted, retry after a while")
            scheduler.scheduleOnce(3.seconds, self, job)
        }
  }

  def shuttingDown: Receive =
  {
    case _ => // ignore all messages, shutting down cluster.
  }
}

object DownloadClient
{
  case object Finished
}
