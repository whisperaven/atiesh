/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.io.File
// scala
import scala.util.Try
// typesafe config
import com.typesafe.config.{ Config, ConfigFactory, ConfigParseOptions, ConfigSyntax }

object ConfigParser {
  def parseConfigFile(cfp: File): Try[Configuration] = Try {
    if (!cfp.exists() || !cfp.isFile()) {
      throw new ConfigParseException(s"configuration file ${cfp.getAbsolutePath()} does not exists or not a file")
    } else if (!cfp.canRead()) {
      throw new ConfigParseException(s"can not open file ${cfp.getAbsolutePath()} for read (maybe permission deny)")
    } else {
      val opt = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
      val cfg = ConfigFactory.load(ConfigFactory.parseFile(cfp, opt))
      Configuration(cfg)
    }
  }
}
