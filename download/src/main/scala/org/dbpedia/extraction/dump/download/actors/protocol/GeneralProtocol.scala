package org.dbpedia.extraction.dump.download.actors.protocol

object GeneralProtocol
{
  // This message is used by different actors to propagate a cluster shutdown.
  case object ShutdownCluster
}
