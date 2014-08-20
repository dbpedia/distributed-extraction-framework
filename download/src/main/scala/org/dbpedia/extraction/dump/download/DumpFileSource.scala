package org.dbpedia.extraction.dump.download

import org.dbpedia.extraction.util.{Language, WikiInfo}
import scala.io.{Source, Codec}
import java.net.URL
import scala.collection.mutable
import scala.collection.immutable.SortedSet
import scala.collection.mutable.{ListBuffer, Set}
import org.apache.hadoop.fs.Path
import org.dbpedia.extraction.dump.download.actors.message.DumpFile

/**
 * Generate DumpFile objects each representing a specific wiki file to download.
 * Most of the code was taken from LanguageDownloader (extraction-framework).
 *
 * TODO: Integrate this to LanguageDownloader and reuse it here (reduce code duplication)?
 */
class DumpFileSource(languages: mutable.HashMap[Language, mutable.Set[(String, Boolean)]],
                     baseUrl: URL,
                     baseDir: Path,
                     wikiSuffix: String,
                     ranges: mutable.HashMap[(Int, Int), mutable.Set[(String, Boolean)]],
                     dateRange: (String, String),
                     dumpCount: Int)
  extends Traversable[DumpFile] with Iterable[DumpFile]
{
  private val DateLink = """<a href="(\d{8})/">""".r
  private val list = new ListBuffer[DumpFile]()

  override def iterator: Iterator[DumpFile] = list.iterator

  override def foreach[U](func: DumpFile => U)
  {
    if(list.isEmpty)
    {
      // resolve page count ranges to languages
      if (ranges.nonEmpty)
      {
        val wikis = WikiInfo.fromURL(WikiInfo.URL, Codec.UTF8)

        // for all wikis in one of the desired ranges...
        for (((from, to), files) <- ranges; wiki <- wikis; if from <= wiki.pages && wiki.pages <= to)
        {
          // ...add files for this range to files for this language
          languages.getOrElseUpdate(wiki.language, new mutable.HashSet[(String, Boolean)]) ++= files
        }
      }

      // sort them to have reproducible behavior
      val languageKeys = SortedSet.empty[Language] ++ languages.keys
      languageKeys.foreach
      {
        lang =>
          val done = languageKeys.until(lang)
          val todo = languageKeys.from(lang)
          println("done: " + done.size + " - " + done.map(_.wikiCode).mkString(","))
          println("todo: " + todo.size + " - " + languageKeys.from(lang).map(_.wikiCode).mkString(","))
          for(dumpFile <- LanguageDumpFileSource(lang))
            list += dumpFile
      }
    }
    list foreach func
  }

  private class LanguageDumpFileSource(language: Language) extends Traversable[DumpFile]
  {
    val wiki = language.filePrefix + wikiSuffix
    val mainPage = new URL(baseUrl, wiki + "/")
    val fileNames = languages(language)

    override def foreach[U](func: DumpFile => U)
    {
      forDates(dateRange, dumpCount, func)
    }

    def forDates[U](dateRange: (String, String), dumpCount: Int, func: DumpFile => U)
    {
      val (firstDate, lastDate) = dateRange

      var dates = SortedSet.empty(Ordering[String].reverse)
      for (line <- Source.fromURL(mainPage).getLines())
        DateLink.findAllIn(line).matchData.foreach(dates += _.group(1))

      if (dates.size == 0) throw new Exception("found no date - " + mainPage + " is probably broken or unreachable. check your network / proxy settings.")

      var count = 0

      // find date pages that have all files we want
      for (date <- dates)
      {
        if (count < dumpCount && date >= firstDate && date <= lastDate && forDate(date, func)) count += 1
      }

      if (count == 0) throw new Exception("found no date on " + mainPage + " in range " + firstDate + "-" + lastDate + " with files " + fileNames.mkString(","))
    }

    def forDate[U](date: String, func: DumpFile => U): Boolean =
    {
      val datePage = new URL(mainPage, date + "/") // here we could use index.html
    val datePageLines = Source.fromURL(datePage).getLines().toTraversable

      // Collect regexes
      val regexes = fileNames.filter(_._2).map(_._1)
      val fileNamesFromRegexes = expandFilenameRegex(date, datePageLines, regexes)
      val staticFileNames = fileNames.filter(!_._2).map(_._1)

      val allFileNames = fileNamesFromRegexes ++ staticFileNames
      //      val urls = allFileNames.map(fileName => new URL(baseURL, wiki + "/" + date + "/" + wiki + "-" + date + "-" + fileName))
      val dumpFiles = allFileNames.map(fileName => DumpFile(baseDir.toUri.getPath, wikiSuffix, language.wikiCode, date, fileName))


      // all the links we need - only for non regexes (we have already checked regex ones)
      val links = new mutable.HashMap[String, String]()
      for (fileName <- staticFileNames) links(fileName) = "<a href=\"/" + wiki + "/" + date + "/" + wiki + "-" + date + "-" + fileName + "\">"
      // Here we should set "<a href=\"/"+wiki+"/"+date+"/"+wiki+"-"+date+"-"+fileName+"\">"
      // but "\"/"+wiki+"/"+date+"/" does not exists in incremental updates, keeping the trailing "\">" should do the trick
      // for (fileName <- fileNames) links(fileName) = wiki+"-"+date+"-"+fileName+"\">"

      for (line <- datePageLines)
        links.foreach
        {
          case (fileName, link) => if (line contains link) links -= fileName
        }

      // did we find them all?
      // Fail if:
      // - the user specified static file names and not all of them have been found
      // OR
      // - the user specified regular expressions and no file has been found that satisfied them
      if ((staticFileNames.nonEmpty && links.nonEmpty) || (regexes.nonEmpty && fileNamesFromRegexes.isEmpty))
      {
        // TODO: Fix message
        val staticFilesMessage = if (links.nonEmpty) " has no links to [" + links.keys.mkString(",") + "]" else ""
        val dynamicFilesMessage = if (fileNamesFromRegexes.isEmpty && regexes.nonEmpty) " has no links that satisfies [" + regexes.mkString(",") + "]" else ""
        println("date page '" + datePage + staticFilesMessage + dynamicFilesMessage)
        false
      }
      else
      {
        println("date page '" + datePage + "' has all files [" + allFileNames.mkString(",") + "]")
        // run closure over all DumpFiles
        for (dumpFile <- dumpFiles) func(dumpFile)
        true
      }
    }

    private def expandFilenameRegex(date: String, index: Traversable[String], filenameRegexes: mutable.Set[String]): mutable.Set[String] =
    {
      // Prepare regexes
      val regexes = filenameRegexes.map(regex => ("<a href=\"/" + wiki + "/" + date + "/" + wiki + "-" + date + "-(" + regex + ")\">").r)

      // Result
      val filenames = Set[String]()

      for (line <- index)
        regexes.foreach(regex => regex.findAllIn(line).matchData.foreach(filenames += _.group(1)))

      filenames
    }
  }

  private object LanguageDumpFileSource
  {
    def apply(language: Language) = new LanguageDumpFileSource(language)
  }

}
