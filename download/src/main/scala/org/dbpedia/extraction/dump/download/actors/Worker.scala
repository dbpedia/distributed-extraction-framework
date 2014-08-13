package org.dbpedia.extraction.dump.download.actors

import akka.actor._
import scala.concurrent.duration._
import java.util.UUID
import akka.actor.SupervisorStrategy.{Stop, Restart}
import akka.contrib.pattern.ClusterClient.SendToAll
import scala.Some
import akka.actor.OneForOneStrategy
import org.dbpedia.extraction.dump.download.actors.message.{WorkerProgressMessage, MasterWorkerMessage, GeneralMessage}
import GeneralMessage.ShutdownCluster
import org.dbpedia.extraction.dump.download.actors.Worker.DownloadComplete

/**
 * Worker actor that runs on each worker node.
 *
 * TODO: Add documentation
 */
class Worker(clusterClient: ActorRef, downloadRunnerProps: Props, registerInterval: FiniteDuration)
  extends Actor with ActorLogging
{

  import MasterWorkerMessage._
  import WorkerProgressMessage._
  import context.dispatcher

  def scheduler = context.system.scheduler

  val workerId = UUID.randomUUID().toString

  val registerTask = context.system.scheduler.schedule(0.seconds, registerInterval, clusterClient,
                                                       SendToAll("/user/master/active", RegisterWorker(workerId)))

  val downloadRunner = context.watch(context.actorOf(downloadRunnerProps, "runner"))

  var currentDownloadId: Option[String] = None

  private var totalBytes = 0l
  private var currentBytes = 0l
  private var progressDelays = 0
  private val MaxProgressDelays = 5

  def workId: String = currentDownloadId match
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
        if (registerTask.isCancelled) Stop else Restart
    }

  override def postStop(): Unit = registerTask.cancel()

  def receive = idle

  def idle: Receive =
  {
    case ShutdownCluster =>
      sendToMaster(RemoveWorker(workerId))
      registerTask.cancel()
      context.stop(downloadRunner)
      context.stop(self)
      context.system.shutdown()

    case DownloadIsReady =>
      sendToMaster(WorkerRequestsDownload(workerId))

    case job @ MirroredDownloadJob(_, DownloadJob(downloadId, _)) =>
      log.info("Got download job: {}", job)
      currentDownloadId = Some(downloadId)
      downloadRunner ! job
      context.become(working)
  }

  def working: Receive =
  {
    case ProgressStart(total) =>
      if(totalBytes == 0) totalBytes = total

    case Progress(bytes) =>
      if(bytes > currentBytes) currentBytes = bytes else progressDelays += 1
      sendToMaster(ProgressReport(workerId, bytes))
      if(progressDelays > MaxProgressDelays)
      {
        val delay = progressDelays * downloadRunnerProps.args(0).asInstanceOf[FiniteDuration].toSeconds
        throw new Exception(s"Download progress has stagnated. No update occurred in $delay seconds!")
      }

    case DownloadComplete(output, bytes) =>
      log.info("Download is complete. Output file: {}. Total bytes: {}", output, bytes)
      sendToMaster(DownloadIsDone(workerId, workId, output, bytes))
      context.setReceiveTimeout(10.seconds)
      context.become(waitForDownloadIsDoneAck(output, bytes))

    case ShutdownCluster =>
      log.info("Yikes. Master told me to shutdown, while I'm downloading.")

    case _: MirroredDownloadJob =>
      log.info("Yikes. Master told me to do download, while I'm downloading.")
  }

  def waitForDownloadIsDoneAck(outputFilePath: String, bytes: Long): Receive =
  {
    case Ack(id) if id == workId =>
      sendToMaster(WorkerRequestsDownload(workerId))
      context.setReceiveTimeout(Duration.Undefined)
      context.become(idle)
    case ReceiveTimeout =>
      log.info("No ack from master, retrying")
      sendToMaster(DownloadIsDone(workerId, workId, outputFilePath, bytes))
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
  def props(clusterClient: ActorRef, downloadRunnerProps: Props, registerInterval: FiniteDuration = 10.seconds): Props =
    Props(classOf[Worker], clusterClient, downloadRunnerProps, registerInterval)

  case class DownloadComplete(outputFilePath: String, bytes: Long)

}