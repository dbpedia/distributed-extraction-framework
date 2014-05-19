package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.dbpedia.extraction.sources.WikiPage
import com.esotericsoftware.kryo.io.{Output, Input}
import org.dbpedia.extraction.wikiparser.WikiTitle

/**
 * Kryo serializer for org.dbpedia.extraction.sources.WikiPage
 */
class WikiPageSerializer extends Serializer[WikiPage]
{
  override def write(kryo: Kryo, output: Output, wikiPage: WikiPage)
  {
    kryo.writeObjectOrNull(output, wikiPage.title, new WikiTitleSerializer)
    kryo.writeObjectOrNull(output, wikiPage.redirect, new WikiTitleSerializer)
    output.writeLong(wikiPage.id)
    output.writeLong(wikiPage.revision)
    output.writeLong(wikiPage.timestamp)
    output.writeLong(wikiPage.contributorID)
    output.writeString(wikiPage.contributorName)
    output.writeString(wikiPage.source)
    output.writeString(wikiPage.format)
  }

  override def read(kryo: Kryo, input: Input, wikiPageClass: Class[WikiPage]): WikiPage =
  {
    val title = kryo.readObjectOrNull(input, classOf[WikiTitle], new WikiTitleSerializer)
    val redirect = kryo.readObjectOrNull(input, classOf[WikiTitle], new WikiTitleSerializer)
    val id = input.readLong()
    val revision = input.readLong()
    val timestamp = input.readLong()
    val contributorID = input.readLong()
    val contributorName = input.readString()
    val source = input.readString()
    val format = input.readString()
    new WikiPage(title, redirect, id, revision, timestamp, contributorID, contributorName, source, format)
  }
}
