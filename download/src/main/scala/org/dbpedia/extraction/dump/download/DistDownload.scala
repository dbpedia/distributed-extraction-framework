package org.dbpedia.extraction.dump.download

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterClient}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.language.postfixOps
import org.dbpedia.extraction.dump.download.actors._
import akka.actor.RootActorPath
import scala.Some
import java.util.logging.Logger
import org.dbpedia.extraction.util.RemoteExecute
import org.dbpedia.extraction.dump.download.actors.DownloadClient.Finished

/**
 * Distributed Wikipedia dump downloader.
 *
 * While running this on a cluster, make sure that all configuration variables (including the paths to configuration files)
 * are valid in all nodes of the cluster, ie. the configuration files need to be present on the worker nodes too.
 */
object DistDownload extends RemoteExecute
{
  val logger = Logger.getLogger(classOf[DistDownload].getName)

  def main(args: Array[String]): Unit =
  {
    val config = new DistDownloadConfig(args)
    if (config.isMaster)
    {
      val cluster = new ClusterStartup(config)

      // Start master on the driver node
      val joinAddress = cluster.startMaster(None, "driver")
      Thread.sleep(5000) // wait a few sec for master to start up

      (config.privateKey, config.sshPassphrase) match
      {
        case (Some(identity), Some(passphrase)) => // both private key and passphrase are provided
          addIdentity(identity, passphrase)
        case (Some(identity), None) => // passphrase is empty
          addIdentity(identity)
        case _ => // no private key provided
      }

      for (host <- config.slaves)
      {
        val session = createSession(config.userName, host)
        for (worker <- 1 to config.workersPerSlave)
        {
          val command = """cd %s/download;mkdir -p ../logs;nohup ../run download join=%s %s > ../logs/%s-%d.out &""".
                        format(config.homeDir, joinAddress, args.mkString(" "), host, worker)
          println(command)
          println(execute(session, command))
        }
        session.disconnect()
      }

      // Start download client and result/progress consumer
      val client = cluster.startFrontend(joinAddress)
      val dumpFiles = new DumpFileSource(config.languages,
                                         config.baseUrl,
                                         config.baseDir,
                                         config.wikiName,
                                         config.ranges,
                                         config.dateRange,
                                         config.dumpCount)
      for(dumpFile <- dumpFiles)
        client ! dumpFile

      client ! Finished
    }
    else
    {
      val cluster = new ClusterStartup(config)
      cluster.startWorker(config.joinAddress.get)
    }
  }
}

class DistDownload

class ClusterStartup(config: DistDownloadConfig)
{
  def systemName = "Workers"

  private def progressReportTimeout = config.progressReportInterval + 2.seconds

  def startMaster(joinAddressOption: Option[Address], role: String): Address =
  {
    val conf = ConfigFactory.parseString( s"""akka.cluster.roles=[$role]\nakka.remote.netty.tcp.hostname="${config.master}"""").
               withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    val joinAddress = joinAddressOption.getOrElse(Cluster(system).selfAddress)
    Cluster(system).join(joinAddress)
    system.actorOf(
                    ClusterSingletonManager.props(Master.props(
                                                                progressReportTimeout,
                                                                config.mirrors,
                                                                config.threadsPerMirror
                                                              ),
                                                  "active", PoisonPill, Some(role)
                                                 ),
                    "master")
    joinAddress
  }

  def startFrontend(joinAddress: akka.actor.Address): ActorRef =
  {
    val conf = ConfigFactory.parseString( s"""akka.remote.netty.tcp.hostname="${config.master}"""").
               withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    Cluster(system).join(joinAddress)

    val client = system.actorOf(Props[DownloadClient], "client")
    system.actorOf(Props[DownloadResultConsumer], "consumer")
    client
  }

  def startWorker(contactAddress: akka.actor.Address) =
  {
    val conf = ConfigFactory.load()
    val system = ActorSystem(systemName, conf)
    val initialContacts = Set(system.actorSelection(RootActorPath(contactAddress) / "user" / "receptionist"))
    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")
    system.actorOf(
                    Worker.props(clusterClient,
                                 DownloadJobRunner.props(config.progressReportInterval,
                                                         config.hadoopConf,
                                                         config.localTempDir,
                                                         config.unzip
                                                        ),
                                 config.maxDuplicateProgress
                                ),
                    "worker"
                  )
  }
}