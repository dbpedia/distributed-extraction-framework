package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.wikiparser.{Namespace, WikiTitle}
import org.dbpedia.extraction.util.Language

/**
 * Kryo serializer for org.dbpedia.extraction.wikiparser.WikiTitle
 */
class WikiTitleSerializer extends Serializer[WikiTitle]
{
  override def write(kryo: Kryo, output: Output, wikiTitle: WikiTitle)
  {
    output.writeString(wikiTitle.decoded)
    kryo.writeObjectOrNull(output, wikiTitle.language, new LanguageSerializer)
    kryo.writeObjectOrNull(output, wikiTitle.namespace, new FieldSerializer(kryo, classOf[Namespace]))
    output.writeBoolean(wikiTitle.isInterLanguageLink)
    output.writeString(wikiTitle.fragment)
  }

  override def read(kryo: Kryo, input: Input, wikiTitleClass: Class[WikiTitle]): WikiTitle =
  {
    val decoded = input.readString()
    val language = kryo.readObjectOrNull(input, classOf[Language], new LanguageSerializer)
    val namespace = kryo.readObjectOrNull(input, classOf[Namespace], new FieldSerializer(kryo, classOf[Namespace]))
    val isInterLanguageLink = input.readBoolean()
    val fragment = input.readString()
    new WikiTitle(decoded, namespace, language, isInterLanguageLink, fragment)
  }
}
