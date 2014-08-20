package org.dbpedia.extraction.spark.serialize

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import scala.Console._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Namespace, WikiTitle}
import org.dbpedia.extraction.util.Language
import java.util.logging.Logger
import org.dbpedia.extraction.dataparser.ParserUtils

/**
 * It's best to register the classes that will be serialized/deserialized with Kryo.
 */
class KryoExtractionRegistrator extends KryoRegistrator
{
  override def registerClasses(kryo: Kryo)
  {
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[org.dbpedia.extraction.dataparser.GeoCoordinateParser])
    kryo.register(classOf[org.dbpedia.extraction.dataparser.SingleGeoCoordinateParser])
    kryo.register(classOf[org.dbpedia.extraction.destinations.Dataset])
    kryo.register(classOf[org.dbpedia.extraction.destinations.Quad])
    kryo.register(classOf[org.dbpedia.extraction.dump.extract.DistConfigLoader])
    kryo.register(classOf[org.dbpedia.extraction.dump.extract.DumpExtractionContext])
    kryo.register(classOf[org.dbpedia.extraction.dump.extract.DumpExtractionContextWrapper])
    kryo.register(classOf[org.dbpedia.extraction.mappings.ArticleCategoriesExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.ArticlePageExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.ArticleTemplatesExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.CategoryLabelExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.CompositeParseExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.DistRedirects])
    kryo.register(classOf[org.dbpedia.extraction.mappings.ExternalLinksExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.GeoExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.InfoboxExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.InterLanguageLinksExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.LabelExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.PageIdExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.PageLinksExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.ProvenanceExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.RedirectExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.Redirects])
    kryo.register(classOf[org.dbpedia.extraction.mappings.RevisionIdExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.RootExtractor])
    kryo.register(classOf[org.dbpedia.extraction.mappings.SkosCategoriesExtractor])
    kryo.register(classOf[org.dbpedia.extraction.dataparser.ParserUtils])
    kryo.register(classOf[org.dbpedia.extraction.ontology.datatypes.Datatype])
    kryo.register(classOf[org.dbpedia.extraction.ontology.OntologyClass])
    kryo.register(classOf[org.dbpedia.extraction.ontology.OntologyDatatypeProperty])
    kryo.register(classOf[org.dbpedia.extraction.ontology.OntologyObjectProperty])
    kryo.register(classOf[org.dbpedia.extraction.ontology.OntologyProperty])
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[Array[scala.collection.Seq[_]]])
    kryo.register(classOf[scala.runtime.BoxedUnit])
    kryo.register(classOf[Array[scala.Tuple2[_,_]]])
    kryo.register(classOf[scala.util.matching.Regex])
    kryo.register(classOf[WikiPage], new WikiPageSerializer)
    kryo.register(classOf[WikiTitle], new WikiTitleSerializer)
    kryo.register(classOf[Namespace])
    kryo.register(classOf[Language], new LanguageSerializer)
    kryo.register(classOf[Logger], new LoggerSerializer)
    kryo.register(classOf[ParserUtils], new ParserUtilsSerializer)
  }
}