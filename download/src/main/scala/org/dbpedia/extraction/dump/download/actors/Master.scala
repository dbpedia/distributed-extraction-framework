package org.dbpedia.extraction.dump.download.actors

import scala.concurrent.duration.{Deadline, FiniteDuration}
import akka.actor._
import akka.contrib.pattern.{DistributedPubSubMediator, DistributedPubSubExtension}
import scala.collection.immutable.Queue
import org.dbpedia.extraction.dump.download.actors.message.GeneralMessage.ShutdownCluster
import scala.Some
import akka.contrib.pattern.DistributedPubSubMediator.Put
import org.dbpedia.extraction.dump.download.actors.message.MasterWorkerMessage
import java.net.URL

/**
 * Master/driver node actor.
 * TODO: Add documentation
 *
 * @param workTimeout Workers need to send download progress reports within this timeout
 * @param mirrors List of wikipedia mirror URLs
 * @param threadsPerMirror Number of simultaneous downloads per mirror
 */
class Master(workTimeout: FiniteDuration, mirrors: Seq[URL], threadsPerMirror: Int) extends Actor with ActorLogging
{

  import Master._
  import MasterWorkerMessage._
  import context.dispatcher

  def scheduler = context.system.scheduler

  val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Put(self)

  private var workers = Map[String, WorkerState]()
  private var pendingDownloads = Queue[DownloadJob]()
  private var downloadIds = Set[String]()
  private var mirrorsInUse = (mirrors zip Seq.fill(mirrors.size)(0)).toMap // Mapping mirror URL to number of simultaneous downloads

  val cleanupTask = scheduler.schedule(workTimeout / 2, workTimeout / 2,
                                       self, CleanupTick)


  override def postStop(): Unit = cleanupTask.cancel()

  def receive =
  {
    case ShutdownCluster =>
      if (pendingDownloads.isEmpty)
      {
        if (workers.isEmpty)
        {
          log.info("Stopping master!")
          mediator ! DistributedPubSubMediator.Publish(General, ShutdownCluster)
          self ! PoisonPill
          context.stop(self)
          context.system.shutdown()
        } else
        {
          workers.foreach
          {
            case (_, WorkerState(ref, Idle)) => ref ! ShutdownCluster
            case _ =>
          }
          log.info("Some workers still busy! Cannot stop master yet!")
          context.system.scheduler.scheduleOnce(workTimeout, self, ShutdownCluster)
        }
      } else
      {
        log.info("Some work pending! Cannot stop master yet!")
        context.system.scheduler.scheduleOnce(workTimeout, self, ShutdownCluster)
      }

    case RemoveWorker(workerId) =>
      workers -= workerId

    case ProgressReport(workerId: String, progress: Long) =>
      log.info("Heard from worker {}: {} ", workerId, progress)
      workers.get(workerId) match
      {
        case Some(s@WorkerState(_, Busy(downloadJob, deadline))) =>
          mediator ! DistributedPubSubMediator.Publish(ProgressTopic, DownloadProgress(downloadJob, progress))
          workers -= workerId
          workers += (workerId -> WorkerState(sender, status = Busy(downloadJob, Deadline.now + workTimeout)))

        case x => log.info("NOTHING RECEIVNEKJNF" + x)
      }

    case RegisterWorker(workerId) =>
      if (workers.contains(workerId))
      {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      } else
      {
        log.info("Worker registered: {}", workerId)
        workers += (workerId -> WorkerState(sender, status = Idle))
        if (pendingDownloads.nonEmpty)
          sender ! DownloadIsReady
      }

    case WorkerRequestsDownload(workerId) =>
      if (pendingDownloads.nonEmpty)
      {
        workers.get(workerId) match
        {
          case Some(s@WorkerState(_, Idle)) =>
            getFreeMirror foreach
            {
              case url => // We have a free mirror!
                val (downloadJob, rest) = pendingDownloads.dequeue
                pendingDownloads = rest
                val downloadWithMirror = MirroredDownloadJob(url, downloadJob)
                log.info("Giving worker {} a download job {}", workerId, downloadWithMirror)
                // TODO store in Eventsourced
                sender ! downloadWithMirror
                mirrorsInUse += (url -> (mirrorsInUse(url) + 1))
                workers += (workerId -> s.copy(status = Busy(downloadWithMirror, Deadline.now + workTimeout)))
            }
          case _ =>
        }
      }

    case DownloadIsDone(workerId, downloadId, outputPath, totalBytes) =>
      workers.get(workerId) match
      {
        case Some(s@WorkerState(_, Busy(downloadJob, _))) if downloadJob.job.downloadId == downloadId =>
          log.info("Download is done: {} => {},{} by worker {}", downloadJob, outputPath, totalBytes, workerId)
          // TODO store in Eventsourced

          val mirror = downloadJob.baseUrl
          mirrorsInUse += (mirror -> (mirrorsInUse(mirror) - 1))
          workers += (workerId -> s.copy(status = Idle))

          mediator ! DistributedPubSubMediator.Publish(ResultsTopic, DownloadResult(downloadJob, outputPath, totalBytes))

          sender ! MasterWorkerMessage.Ack(downloadId)
        case _ =>
          if (downloadIds.contains(downloadId))
          {
            // previous Ack was lost, confirm again that this is done
            sender ! MasterWorkerMessage.Ack(downloadId)
          }
      }

    case DownloadFailed(workerId, downloadId) =>
      workers.get(workerId) match
      {
        case Some(s@WorkerState(_, Busy(downloadJob, _))) if downloadJob.job.downloadId == downloadId =>
          log.info("Download failed: {}", downloadJob)
          // TODO store in Eventsourced
          val mirror = downloadJob.baseUrl
          mirrorsInUse += (mirror -> (mirrorsInUse(mirror) - 1))
          workers += (workerId -> s.copy(status = Idle))

          pendingDownloads = pendingDownloads enqueue downloadJob.job
          notifyWorkers()
        case _ =>
      }

    case job: DownloadJob =>
      // idempotent
      if (downloadIds.contains(job.downloadId))
      {
        sender ! Master.Ack(job.downloadId)
      } else
      {
        log.info("Accepted download: {}", job)
        // TODO store in Eventsourced
        pendingDownloads = pendingDownloads enqueue job
        downloadIds += job.downloadId
        sender ! Master.Ack(job.downloadId)
        notifyWorkers()
      }

    case CleanupTick =>
      for ((workerId, s@WorkerState(_, Busy(downloadJob, timeout))) <- workers)
      {
        if (timeout.isOverdue)
        {
          log.info("Download timed out: {}", downloadJob)
          // TODO store in Eventsourced
          workers -= workerId
          pendingDownloads = pendingDownloads enqueue downloadJob.job
          notifyWorkers()
        }
      }
  }

  def getFreeMirror: Option[URL] =
    mirrorsInUse.find(_._2 < threadsPerMirror) match
    {
      case Some((url, _)) => Some(url)
      case _ => None
    }

  def notifyWorkers(): Unit =
    if (pendingDownloads.nonEmpty)
    {
      // could pick a few random instead of all
      workers.foreach
      {
        case (_, WorkerState(ref, Idle)) => ref ! DownloadIsReady
        case _ => // busy
      }
    }

  // TODO cleanup old workers
  // TODO cleanup old downloadIds
}

object Master
{
  val ResultsTopic = "results"
  val ProgressTopic = "progress"
  val General = "general"

  def props(workTimeout: FiniteDuration, mirrors: Seq[URL], threadsPerMirror: Int): Props =
    Props(classOf[Master], workTimeout, mirrors, threadsPerMirror)

  case class Ack(downloadId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(job: MirroredDownloadJob, deadline: Deadline) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus)

  private case object CleanupTick

}