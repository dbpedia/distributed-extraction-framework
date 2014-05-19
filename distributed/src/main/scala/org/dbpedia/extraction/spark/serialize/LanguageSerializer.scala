package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.util.Language

/**
 * Kryo serializer for org.dbpedia.extraction.util.Language
 */
class LanguageSerializer extends Serializer[Language]
{
  override def write(kryo: Kryo, output: Output, language: Language)
  {
    output.writeString(language.wikiCode)
  }

  override def read(kryo: Kryo, input: Input, languageClass: Class[Language]): Language =
  {
    val wikiCode = input.readString()
    Language(wikiCode)
  }
}
