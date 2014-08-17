package org.dbpedia.extraction.dump.download.actors

import akka.actor._
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration
import org.dbpedia.extraction.dump.download.actors.message.{DownloaderProgressMessage, WorkerProgressMessage}
import scala.Some

/**
 * An actor that receives Start and Read messages, and relays ProgressStart and Progress messages to the client.
 * This is used to keep track of download progress - the number of bytes being read in real time.
 *
 * @param client The actor to send progress messages to
 * @param notifyInterval The time interval at which progress reports will be sent to client
 */
class DownloadProgressTracker(client: ActorRef, notifyInterval: FiniteDuration) extends Actor with ActorLogging
{
  import WorkerProgressMessage._
  import DownloaderProgressMessage._
  import DownloadProgressTracker._
  import context.dispatcher

  def scheduler = context.system.scheduler

  private val bytesRead = new AtomicLong()

  /** This task is used to send Progress messages to client at each interval */
  private var progressTaskOption: Option[Cancellable] = None

  override def postStop() = progressTaskOption.foreach(_.cancel())

  def receive =
  {
    case Start(total) => // Sent by ActoredCounter to signal start of download
      if (0 != bytesRead.get() || progressTaskOption.isDefined)
      {
        log.info("ProgressTracker is already started!")
      }
      else
      {
        progressTaskOption = Some(scheduler.schedule(notifyInterval, notifyInterval, self, Tick))
        client ! ProgressStart(total)
      }

    case Read(bytes) => // Sent by ActoredCounter to signal bytes read
      bytesRead.set(bytes)

    case Stop =>
      (progressTaskOption, bytesRead.get) match
      {
        case (Some(progressTask), b) if b != 0 =>
          sender ! ProgressEnd(bytesRead.get())
          bytesRead.set(0)

          progressTask.cancel()
          progressTaskOption = None

        case _ =>
          log.info("ProgressTracker is already stopped!")
      }

    case Tick =>
      client ! Progress(bytesRead.get())
  }
}

object DownloadProgressTracker
{
  case object Tick
}