package org.dbpedia.extraction.spark.serialize

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import scala.Console._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Namespace, WikiTitle}
import org.dbpedia.extraction.util.Language

/**
 * It's best to register the classes that will be serialized/deserialized with Kryo.
 */
class KryoExtractionRegistrator extends KryoRegistrator
{
  override def registerClasses(kryo: Kryo)
  {
    println("Called DBpedia registrators")
    kryo.register(classOf[WikiPage], new WikiPageSerializer)
    kryo.register(classOf[WikiTitle], new WikiTitleSerializer)
    kryo.register(classOf[Namespace])
    kryo.register(classOf[Language], new LanguageSerializer)
  }
}