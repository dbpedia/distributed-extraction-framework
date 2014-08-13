package org.dbpedia.extraction.dump.download.actors.message

object GeneralMessage
{
  // This message is used by different actors to propagate a cluster shutdown.
  case object ShutdownCluster
}
