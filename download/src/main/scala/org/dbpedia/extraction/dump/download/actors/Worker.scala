package org.dbpedia.extraction.dump.download.actors

import akka.actor._
import scala.concurrent.duration._
import java.util.UUID
import akka.actor.SupervisorStrategy.{Stop, Restart}
import org.dbpedia.extraction.dump.download.actors.message._
import GeneralMessage.ShutdownCluster
import scala.language.postfixOps
import org.dbpedia.extraction.dump.download.actors.Worker.DownloadComplete
import scala.Some
import akka.actor.OneForOneStrategy
import akka.contrib.pattern.ClusterClient.SendToAll
import org.dbpedia.extraction.dump.download.actors.message.DownloadJob
import akka.actor.Terminated
import akka.actor.DeathPactException

/**
 * Worker actor that runs on each worker node. This dispatches a download job to a child DownloadJobRunner actor
 * which manages download and a DownloadProgressTracker to send progress reports back to the Worker.
 *
 * @param clusterClient Akka ClusterClient that acts as a proxy to the master
 * @param downloadRunnerProps Props for the downloadRunner actor. See Worker.props()
 * @param maxDuplicateProgress Maximum number of consecutive duplicate progress read bytes to tolerate
 * @param registerInterval The worker registers itself with the master every registerInterval
 */
class Worker(clusterClient: ActorRef, downloadRunnerProps: Props, maxDuplicateProgress: Int, registerInterval: FiniteDuration)
  extends Actor with ActorLogging
{

  import MasterWorkerMessage._
  import WorkerProgressMessage._
  import context.dispatcher

  def scheduler = context.system.scheduler

  val workerId = UUID.randomUUID().toString

  // Register to the master at specific intervals.
  val registerTask = context.system.scheduler.schedule(0.seconds, registerInterval, clusterClient,
                                                       SendToAll("/user/master/active", RegisterWorker(workerId)))

  val downloadRunner = context.watch(context.actorOf(downloadRunnerProps, "runner"))

  var currentDownloadId: Option[String] = None

  private var totalBytes = 0l
  private var currentBytes = 0l
  private var progressDelays = 0

  def downloadId: String = currentDownloadId match
  {
    case Some(workId) => workId
    case None => throw new IllegalStateException("Not working")
  }

  override def supervisorStrategy =
    OneForOneStrategy()
    {
      case _: ActorInitializationException => Stop
      case _: DeathPactException => Stop
      case _: Exception =>
        currentDownloadId foreach (workId => sendToMaster(DownloadFailed(workerId, workId)))
        context.become(idle)
        Restart
    }

  override def postStop(): Unit = registerTask.cancel()

  def receive = idle

  def idle: Receive =
  {
    case ShutdownCluster => // Master sends ShutdownCluster
      sendToMaster(RemoveWorker(workerId))
      scheduler.scheduleOnce(5 seconds)
      {
        registerTask.cancel()
        context.stop(downloadRunner)
        context.stop(self)
        context.system.shutdown()
      }

    case DownloadIsReady => // begin 3-way handshake to get download job from master
      sendToMaster(WorkerRequestsDownload(workerId))

    case job @ MirroredDownloadJob(_, DownloadJob(downloadId, _)) => // receive new download job
      log.info("Got download job: {}", job)
      currentDownloadId = Some(downloadId)

      // reset state variables for new download job
      currentBytes = 0
      totalBytes = 0
      progressDelays = 0

      downloadRunner ! job
      context.become(working)
  }

  def working: Receive =
  {
    case p @ ProgressStart(total) =>
      sendToMaster(ProgressReport(workerId, downloadId, p))
      if(totalBytes == 0) totalBytes = total

    case p @ Progress(bytes) =>
      sendToMaster(ProgressReport(workerId, downloadId, p))

      // check if number of bytes downloaded has increased.
      if(bytes > currentBytes)
      {
        currentBytes = bytes
        progressDelays = 0
      }
      else
      {
        progressDelays += 1
      }

      if(progressDelays > maxDuplicateProgress && totalBytes != bytes) // too many progress delays?
      {
        val delay = progressDelays * downloadRunnerProps.args(0).asInstanceOf[FiniteDuration].toSeconds
        log.info(s"Download progress of $currentDownloadId has stagnated. No update occurred in $delay seconds!")
        sendToMaster(DownloadFailed(workerId, currentDownloadId.get))
      }

    case DownloadComplete(output, bytes) => // DownloadJobRunner sends this upon completion
      log.info("Download is complete. Output file: {}. Total bytes: {}", output, bytes)
      sendToMaster(DownloadIsDone(workerId, downloadId, output, bytes))
      context.setReceiveTimeout(10.seconds)
      context.become(waitForDownloadIsDoneAck(output, bytes)) // Send news of finished download to Master and wait for ACK.

    case ShutdownCluster =>
      log.info("Yikes. Master told me to shutdown, while I'm downloading.")

    case _: MirroredDownloadJob =>
      log.info("Yikes. Master gave me a download job, while I'm downloading.")
  }

  def waitForDownloadIsDoneAck(outputFilePath: String, bytes: Long): Receive =
  {
    case Ack(id) if id == downloadId =>
      sendToMaster(WorkerRequestsDownload(workerId))
      context.setReceiveTimeout(Duration.Undefined)
      context.become(idle)
    case ReceiveTimeout =>
      log.info("No ACK from master, retrying")
      sendToMaster(DownloadIsDone(workerId, downloadId, outputFilePath, bytes))
  }

  override def unhandled(message: Any): Unit = message match
  {
    case Terminated(`downloadRunner`) => context.stop(self)
    case DownloadIsReady =>
    case _ => super.unhandled(message)
  }

  def sendToMaster(msg: Any): Unit =
  {
    clusterClient ! SendToAll("/user/master/active", msg)
  }
}

object Worker
{
  def props(clusterClient: ActorRef, downloadRunnerProps: Props, maxDuplicateProgress: Int, registerInterval: FiniteDuration = 10.seconds): Props =
    Props(classOf[Worker], clusterClient, downloadRunnerProps, maxDuplicateProgress, registerInterval)

  case class DownloadComplete(outputFilePath: String, bytes: Long)

}