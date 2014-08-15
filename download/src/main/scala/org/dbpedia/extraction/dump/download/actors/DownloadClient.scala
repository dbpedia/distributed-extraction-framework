package org.dbpedia.extraction.dump.download.actors

import akka.actor.{ActorLogging, Actor}
import java.util.UUID
import scala.concurrent.duration._
import akka.pattern._
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Send
import org.dbpedia.extraction.dump.download.actors.message.GeneralMessage.ShutdownCluster
import akka.util.Timeout

/**
 * A client actor used to submit download jobs to the master.
 */
class DownloadClient extends Actor with ActorLogging
{

  import context.dispatcher

  def scheduler = context.system.scheduler

  def nextDownloadId(): String = UUID.randomUUID().toString

  val mediator = DistributedPubSubExtension(context.system).mediator

  implicit val timeout = Timeout(10.seconds)

  def receive =
  {
    case ShutdownCluster =>
      mediator ! Send("/user/master/active", ShutdownCluster, localAffinity = false)
      context.stop(self)
      context.system.shutdown()

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
}