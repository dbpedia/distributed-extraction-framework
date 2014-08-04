package worker

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.Terminated
import akka.contrib.pattern.ClusterClient.SendToAll
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Stop
import akka.actor.SupervisorStrategy.Restart
import akka.actor.ActorInitializationException
import akka.actor.DeathPactException

object Worker {

  def props(clusterClient: ActorRef, workExecutorProps: Props, registerInterval: FiniteDuration = 10.seconds): Props =
    Props(classOf[Worker], clusterClient, workExecutorProps, registerInterval)

  case class WorkComplete(result: Any)
}

class Worker(clusterClient: ActorRef, workExecutorProps: Props, registerInterval: FiniteDuration)
  extends Actor with ActorLogging {
  import Worker._
  import MasterWorkerProtocol._

  val workerId = UUID.randomUUID().toString

  import context.dispatcher
  val registerTask = context.system.scheduler.schedule(0.seconds, registerInterval, clusterClient,
    SendToAll("/user/master/active", RegisterWorker(workerId)))
    
  var progressCounter = 0
  
  val workExecutor = context.watch(context.actorOf(workExecutorProps, "exec"))

  var currentWorkId: Option[String] = None
  def workId: String = currentWorkId match {
    case Some(workId) => workId
    case None         => throw new IllegalStateException("Not working")
  }

  override def supervisorStrategy = OneForOneStrategy() {
    case _: ActorInitializationException => Stop
    case _: DeathPactException           => Stop
    case _: Exception =>
      currentWorkId foreach { workId => sendToMaster(WorkFailed(workerId, workId)) }
      context.become(idle)
      Restart
  }
  
  override def preStart() =
    context.system.scheduler.scheduleOnce(500 millis, self, "reportProgress")
    
  // override postRestart so we don't call preStart and schedule a new message
  override def postRestart(reason: Throwable) = {}

  override def postStop(): Unit = registerTask.cancel()

  def receive = idle

  def idle: Receive = {
    case "reportProgress" =>
      log.info("PROGRESSCOUNTER = " + progressCounter)
      sendToMaster(ProgressReport(workerId, progressCounter))
      progressCounter += 1
      context.system.scheduler.scheduleOnce(2 seconds, self, "reportProgress")
      
    case WorkIsReady =>
      sendToMaster(WorkerRequestsWork(workerId))

    case Work(workId, job) =>
      log.debug("Got work: {}", job)
      currentWorkId = Some(workId)
      workExecutor ! job
      context.become(working)
  }

  def working: Receive = {
    case "reportProgress" =>
      log.info("PROGRESSCOUNTER = " + progressCounter)
      sendToMaster(ProgressReport(workerId, progressCounter))
      progressCounter += 1
      context.system.scheduler.scheduleOnce(2 seconds, self, "reportProgress")
      
    case WorkComplete(result) =>
      log.info("Work is complete. Result {}.", result)
      sendToMaster(WorkIsDone(workerId, workId, result))
      context.setReceiveTimeout(10.seconds)
      context.become(waitForWorkIsDoneAck(result))

    case _: Work =>
      log.info("Yikes. Master told me to do work, while I'm working.")
  }

  def waitForWorkIsDoneAck(result: Any): Receive = {
    case Ack(id) if id == workId =>
      sendToMaster(WorkerRequestsWork(workerId))
      context.setReceiveTimeout(Duration.Undefined)
      context.become(idle)
    case ReceiveTimeout =>
      log.info("No ack from master, retrying")
      sendToMaster(WorkIsDone(workerId, workId, result))
  }

  override def unhandled(message: Any): Unit = message match {
    case Terminated(`workExecutor`) => context.stop(self)
    case WorkIsReady                =>
    case _                          => super.unhandled(message)
  }

  def sendToMaster(msg: Any): Unit = {
    clusterClient ! SendToAll("/user/master/active", msg)
  }

}