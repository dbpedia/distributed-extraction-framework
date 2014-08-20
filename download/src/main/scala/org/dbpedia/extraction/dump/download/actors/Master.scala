package org.dbpedia.extraction.dump.download.actors

import scala.concurrent.duration.{Deadline, FiniteDuration}
import akka.actor._
import akka.contrib.pattern.{DistributedPubSubMediator, DistributedPubSubExtension}
import scala.collection.immutable.Queue
import org.dbpedia.extraction.dump.download.actors.message.GeneralMessage.{MasterQueueEmpty, ShutdownCluster}
import org.dbpedia.extraction.dump.download.actors.message._
import java.net.URL
import scala.Some
import akka.contrib.pattern.DistributedPubSubMediator.Put

/**
 * Master/driver node actor. This is responsible for accepting download jobs from a client and dividing jobs
 * among the different Workers, keeping track of download jobs, handling failed jobs, shutting down the cluster etc.
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

  // The DownloadClient and DownloadResultConsumer communicate with the Master through the DistributedPubSubMediator
  val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Put(self)

  private var workers = Map[String, WorkerState]()
  private var pendingDownloads = Queue[DownloadJob]()
  private var downloadIds = Set[String]()

  // Keep track of the number of simultaneous downloads per mirror.
  private var mirrorsInUse = (mirrors zip Seq.fill(mirrors.size)(0)).toMap // Mapping mirror URL to number of simultaneous downloads

  val cleanupTask = scheduler.schedule(workTimeout / 2, workTimeout / 2,
                                       self, CleanupTick)

  override def postStop(): Unit = cleanupTask.cancel()

  def receive =
  {
    case ShutdownCluster =>
      if (pendingDownloads.isEmpty) // all downloads have finished?
      {
        if (workers.isEmpty) // all workers have been unregistered?
        {
          log.info("Stopping master!")
          mediator ! DistributedPubSubMediator.Publish(General, ShutdownCluster)
          self ! PoisonPill
          context.stop(self)
          context.system.shutdown()
        }
        else
        {
          workers.foreach // still have registered workers?
          {
            case (workerId, WorkerState(ref, Idle)) => // send shutdown signal to idle workers and remove them.
              ref ! ShutdownCluster
              workers -= workerId
            case _ => // come back to the busy worker after a period of workTimeout
          }
          log.debug("Some workers still busy! Cannot stop master yet!")
          context.system.scheduler.scheduleOnce(workTimeout, self, ShutdownCluster)
        }
      }
      else
      {
        log.debug("Some work pending! Cannot stop master yet!")
        context.system.scheduler.scheduleOnce(workTimeout, self, ShutdownCluster)
      }

    case RemoveWorker(workerId) =>
      workers -= workerId

    case p @ ProgressReport(workerId, downloadId, progress) => // Workers send download progress reports at specific intervals
      log.debug("Heard from worker {}: {} ", workerId, progress)
      mediator ! DistributedPubSubMediator.Publish(ProgressTopic, DownloadProgress(downloadId, progress))
      workers.get(workerId) match
      {
        case Some(s@WorkerState(_, Busy(downloadJob, deadline))) =>
          workers += (workerId -> WorkerState(sender, status = Busy(downloadJob, Deadline.now + workTimeout))) // Renew current job deadline
        case _ =>
      }

    case RegisterWorker(workerId) => // Workers register themselves to the master at specific intervals
      if (workers.contains(workerId))
      {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      }
      else
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
          case Some(s@WorkerState(_, Idle)) => // is the requesting Worker Idle?
            getFreeMirror foreach
            {
              case url => // We have a free mirror!
                val (downloadJob, rest) = pendingDownloads.dequeue
                pendingDownloads = rest
                val downloadWithMirror = MirroredDownloadJob(url, downloadJob)

                // Publish new download job so that DownloadResultConsumer can keep track of it
                mediator ! DistributedPubSubMediator.Publish(ProgressTopic, downloadWithMirror)

                sender ! downloadWithMirror // send new download job back to the Worker that sent the job request
                log.info("Giving worker {} a download job {}", workerId, downloadWithMirror)

                mirrorsInUse += (url -> (mirrorsInUse(url) + 1)) // decrement no. of threads to mirror
                workers += (workerId -> s.copy(status = Busy(downloadWithMirror, Deadline.now + workTimeout))) // set worker status to Busy
            }
          case _ =>
        }
      }

    case DownloadIsDone(workerId, downloadId, outputPath, totalBytes) =>
      workers.get(workerId) match
      {
        case Some(s@WorkerState(_, Busy(downloadJob, _))) if downloadJob.job.downloadId == downloadId =>
          log.debug("Download is done: {} => {} bytes written to {} by worker {}", downloadJob, totalBytes, outputPath, workerId)

          val mirror = downloadJob.baseUrl
          mirrorsInUse += (mirror -> (mirrorsInUse(mirror) - 1)) // decrement no. of threads to mirror
          workers += (workerId -> s.copy(status = Idle)) // set worker status to Idle

          // publish download result for DownloadResultConsumer to read
          mediator ! DistributedPubSubMediator.Publish(ResultsTopic, DownloadResult(downloadJob, outputPath, totalBytes))

          sender ! MasterWorkerMessage.Ack(downloadId) // Ack to worker
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

          val mirror = downloadJob.baseUrl
          mirrorsInUse += (mirror -> (mirrorsInUse(mirror) - 1))
          workers += (workerId -> s.copy(status = Idle))

          pendingDownloads = pendingDownloads enqueue downloadJob.job // put the download back into queue
          notifyWorkers()
        case _ =>
      }

    case job: DownloadJob => // client sent a new DownloadJob
      // idempotent
      if (downloadIds.contains(job.downloadId))
      {
        sender ! Master.Ack(job.downloadId)
      }
      else
      {
        log.info("Accepted download: {}", job)
        pendingDownloads = pendingDownloads enqueue job
        downloadIds += job.downloadId
        sender ! Master.Ack(job.downloadId)
        notifyWorkers()
      }

    case CleanupTick => // runs at fixed intervals, removes timed out jobs
      var hasBusy = false
      for ((workerId, s@WorkerState(_, Busy(downloadJob, timeout))) <- workers)
      {
        hasBusy = true
        if (timeout.isOverdue)
        {
          log.info("Download timed out: {}", downloadJob)
          workers -= workerId
          pendingDownloads = pendingDownloads enqueue downloadJob.job
          notifyWorkers()
        }
      }
      // publish MasterQueueEmpty if there are no pending downloads AND no workers are busy
      if(!hasBusy && pendingDownloads.isEmpty) mediator ! DistributedPubSubMediator.Publish(General, MasterQueueEmpty)
  }

  def getFreeMirror: Option[URL] =
    mirrorsInUse.find(_._2 < threadsPerMirror) match
    {
      case Some((url, _)) => Some(url)
      case _ => None
    }

  /** Tell idle workers that download is ready */
  def notifyWorkers(): Unit =
    if (pendingDownloads.nonEmpty)
    {
      // TODO: Pick workers more intelligently, according to number of bytes downloaded by each worker
      // to encourage better spreading out of downloads over the cluster - better for distributed processing too.
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