/**
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

/**
 * Atiesh Main.
 */
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

    val cfg: Configuration =
      ConfigParser.parseConfigFile(new File(args(0))) match {
        case Success(c) => c
        case Failure(exc) =>
          logger.error("got unexpected error during configuration " +
                       "file parse, exit", exc)
          sys.exit(2)
      }

    try {
      val server = new AtieshServer(cfg)
      val sdhook = sys.addShutdownHook(server.disassemble)

      server.assemble()
      server.awaitShutdown()
    } catch {
      case exc: Throwable =>
        logger.error("exiting atiesh due to unexpected fatal exception", exc)
        sys.exit(1)
    }
    sys.exit(0)
  }
}
