package org.dbpedia.extraction.spark.serialize

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.util.Language
import java.util.logging.Logger

/**
 * Kryo serializer for org.dbpedia.extraction.util.Language
 */
class LoggerSerializer extends Serializer[Logger]
{
  override def write(kryo: Kryo, output: Output, logger: Logger)
  {
    output.writeString(logger.getName)
  }

  override def read(kryo: Kryo, input: Input, loggerClass: Class[Logger]): Logger =
  {
    val className = input.readString()
    Logger.getLogger(className)
  }
}
