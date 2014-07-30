package org.dbpedia.extraction.dump.extract

import org.dbpedia.extraction.ontology.Ontology
import org.dbpedia.extraction.sources.{WikiPage, Source}
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.mappings.{Disambiguations, Redirects, Mappings}

/**
 * A simple wrapper for a DumpExtractionContext object
 * 
 * @param context
 */
class DumpExtractionContextWrapper(context: DumpExtractionContext) extends DumpExtractionContext
{
  override def ontology: Ontology = context.ontology

  override def commonsSource: Source = context.commonsSource

  override def language: Language = context.language

  override def mappingPageSource: Traversable[WikiPage] = context.mappingPageSource

  override def mappings: Mappings = context.mappings

  override def articlesSource: Source = context.articlesSource

  override def redirects: Redirects = context.redirects

  override def disambiguations: Disambiguations = context.disambiguations
}
