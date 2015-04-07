package org.apache.spark.ui.jobs

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerJobStart
import org.dbpedia.extraction.util.StringUtils
import scala.collection.mutable

/**
 * SparkListener implementation that provides real-time logging for jobs, tasks and stages in a
 * friendly way omitting most of the details that can be had using Spark's default logging
 * system.
 *
 * This is in the org.apache.spark.ui.jobs package because it needs to extend
 * org.apache.spark.ui.jobs.JobProgressListener which is private[spark].
 */
class DBpediaJobProgressListener(sc: SparkConf) extends JobProgressListener(sc) with Logging
{
  /**
   * The time when this class was created (usually along with the SparkContext).
   * Milliseconds since midnight, January 1, 1970 UTC.
   */
  val startTime = System.currentTimeMillis()

  val stageNumTasks = mutable.Map[Int, Int]() // Maps stageId to number of tasks

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
  {
    super.onStageSubmitted(stageSubmitted)
    val stage = stageSubmitted.stageInfo
    val numTasks = stage.numTasks
    stageNumTasks.synchronized(stageNumTasks(stage.stageId) = numTasks)
    val time = prettyTime(stage.submissionTime.getOrElse(startTime))
    logInfo("Stage #%d: Starting stage %s with %d tasks at %s".format(stage.stageId, stage.name, numTasks, time))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
  {
    super.onStageCompleted(stageCompleted)
    val stage = stageCompleted.stageInfo
    val time = prettyTime(stage.completionTime.getOrElse(startTime))
    logInfo("Stage #%d: Finished stage %s at %s".format(stage.stageId, stage.name, time))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
  {
    super.onTaskStart(taskStart)
    val executor = taskStart.taskInfo.executorId
    val host = taskStart.taskInfo.host
    val time = prettyTime(taskStart.taskInfo.launchTime)
    val taskId = taskStart.taskInfo.taskId
    val stageId = taskStart.taskInfo.taskId
    // Get TaskInfos for this stage to compute number of tasks
    val numTasks = this.stageIdToInfo.size
    //val numTasks = this.stageIdToTaskInfos(stageId).size
    logInfo("Stage #%d: Started task #%d on host %s, executor %s at %s. Total tasks submitted: %d".format(stageId, taskId, host, executor, time, numTasks))
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
  {
    super.onTaskEnd(taskEnd)
    val time = prettyTime(taskEnd.taskInfo.finishTime)
    val taskId = taskEnd.taskInfo.taskId
    val stageId = taskEnd.stageId
    val totalNumTasks = stageNumTasks(taskEnd.stageId)
    // Get TaskInfos for this stage to compute number of tasks
    val numTasks = this.stageIdToInfo.size
    //val numTasks = this.stageIdToTaskInfos(stageId).size
    // Wrap in try/catch to return 0 if no completed/failed tasks for stageId are found in the maps.
    val finished = try { this.numCompletedStages } catch { case ex: NoSuchElementException =>0 }
    val failed = try { this.numFailedStages } catch { case ex: NoSuchElementException =>0 }
    //val finished = try { this.stageIdToTasksComplete(stageId) } catch { case ex: NoSuchElementException => 0 }
    //val failed = try { this.stageIdToTasksFailed(stageId) } catch { case ex: NoSuchElementException => 0 }
    logInfo("Stage #%d: Finished task #%d at %s. Completed: %d/%d Failed: %d/%d Total Progress: %d/%d".format(stageId, taskId, time, finished, numTasks, failed, numTasks, finished, totalNumTasks))
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit =
  {
    super.onJobStart(jobStart)
    logInfo("Started job #" + jobStart.jobId)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit =
  {
    super.onJobEnd(jobEnd)
    logInfo("Finished job #" + jobEnd.jobId)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit =
  {
    super.onTaskGettingResult(taskGettingResult)
  }

  private def prettyTime(time: Long) = StringUtils.prettyMillis(time - startTime)
}