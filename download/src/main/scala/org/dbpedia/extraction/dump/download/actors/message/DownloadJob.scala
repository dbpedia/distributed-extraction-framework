package org.dbpedia.extraction.dump.download.actors.message

import java.net.URL
import org.dbpedia.extraction.dump.download.actors.message.WorkerProgressMessage.ProgressMessage

/**
 * Download job used by the actor framework.
 *
 * @param downloadId Unique job ID
 * @param file URL information
 */
case class DownloadJob(downloadId: String, file: DumpFile)

/**
 * Download job wrapped along with the mirror to use for downloading.
 * This contains all the information needed by DownloadJobRunner to perform the job.
 *
 * @param baseUrl URL of the mirror to download from
 * @param job download job
 */
case class MirroredDownloadJob(baseUrl: URL, job: DownloadJob)

/**
 * Download information for single wiki dump file.
 *
 * @param baseDir Base directory on Hadoop file system (HDFS for distributed downloads)
 * @param wikiSuffix Wiki name suffix (eg. wiki)
 * @param language Language wikiCode
 * @param date YYYYMMDD date string
 * @param fileName URL file name
 */
case class DumpFile(baseDir: String, wikiSuffix: String, language: String, date: String, fileName: String)

/**
 * Download job used by the actor framework.
 *
 * @param job MirroredDownloadJob
 * @param outputPath Output path name in scheme://path/fileName format
 * @param bytes Total bytes downloaded
 */
case class DownloadResult(job: MirroredDownloadJob, outputPath: String, bytes: Long)

/**
 * Progress reports published by Master.
 *
 * @param downloadId Unique job ID
 * @param progress Progress message
 */
case class DownloadProgress(downloadId: String, progress: ProgressMessage)