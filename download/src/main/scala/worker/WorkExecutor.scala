package worker

import akka.actor.Actor
import scala.concurrent.duration._

class WorkExecutor extends Actor {

  def receive = {
    case n: Int =>
    val s = sender
    import context.dispatcher
    context.system.scheduler.scheduleOnce(2 seconds) {
        val n2 = n * n
        val result = s"$n * $n = $n2"
        s ! Worker.WorkComplete(result)
      }
  }

}