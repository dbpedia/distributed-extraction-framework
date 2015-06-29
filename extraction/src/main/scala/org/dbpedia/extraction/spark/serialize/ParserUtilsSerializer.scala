package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.dataparser.ParserUtils
import org.dbpedia.extraction.util.Language
import scala.language.reflectiveCalls
/**
 * Kryo serializer for org.dbpedia.extraction.dataparser.ParserUtils
 */
class ParserUtilsSerializer extends Serializer[ParserUtils]
{
  override def write(kryo: Kryo, output: Output, parserUtils: ParserUtils) {
    kryo.writeObjectOrNull(output, parserUtils.context.language, new LanguageSerializer)
  }

  override def read(kryo: Kryo, input: Input, parserUtilsClass: Class[ParserUtils]): ParserUtils = {
    val lang = kryo.readObjectOrNull(input, classOf[Language], new LanguageSerializer)
    new ParserUtils(new {def language: Language = lang})
  }
}