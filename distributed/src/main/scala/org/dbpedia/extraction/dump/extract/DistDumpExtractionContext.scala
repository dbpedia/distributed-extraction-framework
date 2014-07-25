package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.util._
import org.dbpedia.extraction.mappings._
import org.dbpedia.extraction.sources._
import org.dbpedia.extraction.ontology.Ontology
import org.apache.spark.broadcast.Broadcast

/**
 * Wrapper for DumpExtractionContext to be used with Spark.
 *
 * Delegates all methods to a DumpExtractionContext broadcast variable.
 */
class DistDumpExtractionContext(contextBroadcast: Broadcast[_ <: DumpExtractionContext]) extends DumpExtractionContext
{
  override def ontology: Ontology = contextBroadcast.value.ontology

  override def commonsSource: Source = contextBroadcast.value.commonsSource

  override def language: Language = contextBroadcast.value.language

  override def mappingPageSource: Traversable[WikiPage] = contextBroadcast.value.mappingPageSource

  override def mappings: Mappings = contextBroadcast.value.mappings

  override def articlesSource: Source = contextBroadcast.value.articlesSource

  override def redirects: Redirects = contextBroadcast.value.redirects

  override def disambiguations: Disambiguations = contextBroadcast.value.disambiguations
}
