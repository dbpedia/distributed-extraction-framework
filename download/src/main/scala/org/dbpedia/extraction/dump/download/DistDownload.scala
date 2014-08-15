package org.dbpedia.extraction.dump.download

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterClient}
import com.typesafe.config.ConfigFactory
import scala.language.postfixOps
import org.dbpedia.extraction.dump.download.actors._
import akka.actor.RootActorPath
import scala.Some

/**
 * Distributed Wikipedia dump downloader.
 *
 * The preferred form of running this launcher is: <Java/Maven command> bind-host=192.168.0.1 distconfig=../download.cfg
 * because bind-host is the only variable that depends on the current machine (master/worker) while the rest remain the same.
 */
object DistDownload extends ClusterStartup
{
  def main(args: Array[String]): Unit =
  {
    //TODO: Complete code for cluster startup and generating download jobs
    val config = new DistDownloadConfig(args)
    if(config.isMaster)
    {
      val cluster = new ClusterStartup(config)
      cluster.startMaster(None, "driver")
      Thread.sleep(5000)
      for(host <- config.slaves)
      {

      }
    }
  }
}

class ClusterStartup(config: DistDownloadConfig)
{
  def systemName = "Workers"

  private def progressReportTimeout = config.progressReportInterval + 2

  def startMaster(joinAddressOption: Option[Address], role: String) =
  {
    //val conf = ConfigFactory.parseString(s"""akka.cluster.roles=[$role]\nakka.remote.netty.hostname="${config.master}"""").
    withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    val joinAddress = joinAddressOption.getOrElse(Cluster(system).selfAddress)
    Cluster(system).join(joinAddress)
    (system.actorOf(ClusterSingletonManager.props(Master.props(progressReportTimeout), "active",
                                                  PoisonPill, Some(role)), "master"), system,
      joinAddress)
  }

  def startWorker(hostname: String, contactAddress: akka.actor.Address) =
  {
    //val conf = ConfigFactory.parseString(s"""akka.remote.netty.hostname="$hostname"""").
    withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    val initialContacts = Set(system.actorSelection(RootActorPath(contactAddress) / "user" / "receptionist"))
    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")
    system.actorOf(
                    Worker.props(clusterClient,
                                 DownloadJobRunner.props(config.progressReportInterval,
                                                         config.hadoopConf,
                                                         config.localTempDir,
                                                         config.unzip
                                                        )
                                ),
                    "worker"
                  )
  }

  def startFrontend(joinAddress: akka.actor.Address) =
  {
    val conf = ConfigFactory.parseString(s"""akka.remote.netty.hostname="${config.master}"""").
               withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    Cluster(system).join(joinAddress)

    val client = system.actorOf(Props[DownloadClient], "client")
    system.actorOf(Props[DownloadResultConsumer], "consumer")
    client
  }


}