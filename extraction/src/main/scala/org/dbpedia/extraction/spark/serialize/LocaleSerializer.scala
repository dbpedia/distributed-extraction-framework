package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Output, Input}
import java.util.Locale

/**
 * Kryo serializer for java.util.Locale
 */
class LocaleSerializer extends Serializer[Locale]
{
  override def write(kryo: Kryo, output: Output, locale: Locale)
  {
    output.writeAscii(locale.getLanguage)
    output.writeAscii(locale.getCountry)
    output.writeAscii(locale.getVariant)
  }

  override def read(kryo: Kryo, input: Input, localeClass: Class[Locale]): Locale =
  {
    new Locale(input.readString(), input.readString(), input.readString())
  }
}
