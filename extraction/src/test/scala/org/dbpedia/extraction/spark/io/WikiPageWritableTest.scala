package org.dbpedia.extraction.spark.io

import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuiteLike
import org.dbpedia.extraction.sources.XMLSource
import scala.xml.XML
import org.dbpedia.extraction.util.Language

@RunWith(classOf[JUnitRunner])
class WikiPageWritableTest extends WritableTest[WikiPageWritable] with FunSuiteLike
{
  test("Verify that serialization-deserialization works properly")
  {
    val samplePage =
      """
        |  <page>
        |    <title>Lèmburg</title>
        |    <ns>0</ns>
        |    <id>13</id>
        |    <redirect title="Limburg" />
        |    <revision>
        |      <id>196988</id>
        |      <parentid>5980</parentid>
        |      <timestamp>2010-01-25T20:24:26Z</timestamp>
        |      <contributor>
        |        <username>PahlesBot</username>
        |        <id>458</id>
        |      </contributor>
        |      <minor />
        |      <comment>Bot: automatisch tekst vervangen  (-#redirect +#REDIRECT)</comment>
        |      <text xml:space="preserve">#REDIRECT [[Limburg]]</text>
        |      <sha1>2uewphqvpum37i9d7g5okf5c3m643c7</sha1>
        |      <model>wikitext</model>
        |      <format>text/x-wiki</format>
        |    </revision>
        |  </page>
      """.stripMargin

    val wikiPage = XMLSource.fromXML(XML.loadString("<mediawiki>" + samplePage + "</mediawiki>"), Language("li")).head
    val writable1 = new WikiPageWritable(wikiPage)
    val writable2 = new WikiPageWritable()

    performReadWriteRoundTrip(writable1, writable2)
    assertEquals(writable1.get.toString, writable2.get.toString)
  }
}
