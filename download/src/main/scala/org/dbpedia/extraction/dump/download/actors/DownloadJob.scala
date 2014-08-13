package org.dbpedia.extraction.dump.download.actors

import java.net.URL
import java.io.File
import org.dbpedia.extraction.util.Language
import org.apache.hadoop.fs.Path

/**
 * Download job wrapped along with the mirror to use for downloading.
 * This contains all the information needed by DownloadJobRunner to perform the job.
 *
 * @param baseUrl URL of the mirror to download from
 * @param job download job
 */
case class MirroredDownloadJob(baseUrl: URL, job: DownloadJob)

/**
 * Download job used by the actor framework.
 *
 * @param downloadId Unique job ID
 * @param file URL information
 */
case class DownloadJob(downloadId: String, file: DumpFile)

/**
 * Download information for single wiki dump file.
 *
 * @param baseDir Base directory on Hadoop file system (HDFS for distributed downloads)
 * @param wikiSuffix Wiki name suffix (eg. wiki)
 * @param language Wiki language
 * @param date YYYYMMDD date string
 * @param fileName File name to save the URL to
 */
case class DumpFile(baseDir: Path, wikiSuffix: String, language: Language, date: String, fileName: String)

/**
 * Download job used by the actor framework.
 *
 * @param job MirroredDownloadJob
 * @param outputPath Output path name in scheme://path/fileName format
 * @param bytes Total bytes downloaded
 */
case class DownloadResult(job: MirroredDownloadJob, outputPath: String, bytes: Long)

/**
 * Progress reports sent from Master to DownloadResultConsumer.
 *
 * @param job MirroredDownloadJob
 * @param bytes Number of bytes downloaded till now
 */
case class DownloadProgress(job: MirroredDownloadJob, bytes: Long)