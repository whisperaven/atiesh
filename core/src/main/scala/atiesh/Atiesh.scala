/*
 * Copyright (C) Hao Feng
 */

package atiesh

// java
import java.io.File
// scala
import scala.util.{ Success, Failure }
// internal
import atiesh.server.AtieshServer
import atiesh.utils.{ ConfigParser, Configuration, Logging }

object Atiesh extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("""
        |Usage: atiesh.Atiesh <config-file-path>
        |  <config-file-path>  path to atiesh.conf, could be absolute or relative path
        |
        |Example:
        |  $ /usr/bin/java -Xms4G -Xmx4G -server \
        |  >   -Dlogback.configurationFile=logback.xml \
        |  >   atiesh.Atiesh atiesh.conf
      """.stripMargin)
      sys.exit(1)
    }

    /* open & read configuration file */
    val cfg: Configuration = ConfigParser.parseConfigFile(new File(args(0))) match {
      case Success(c) => c
      case Failure(exc) =>
        logger.error("got unexpected error during configuration file parse, exit", exc)
        sys.exit(2)
    }

    /* initialize & startup & block server */
    try {
      val server = AtieshServer(cfg)

      sys.addShutdownHook(server.shutdown)

      server.startup()
      server.awaitShutdown()
    } catch {
      case exc: Throwable =>
        logger.error("Exiting atiesh due to unexpected fatal exception", exc)
        sys.exit(1)
    }
    sys.exit(0)
  }
}
