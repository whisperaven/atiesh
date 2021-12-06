/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.time.{ Duration => JDuration }
// scala
import scala.concurrent.duration._
import scala.collection.JavaConverters._
// typesafe config
import com.typesafe.config.{ Config, ConfigValueType, ConfigException }

/**
 * Atiesh configuration wrapper class for typesafe config.
 */
case class Configuration(cfg: Config) {
  def unwrapped(): Config = cfg

  def entrySet(): Set[(String, AnyRef)] = {
    cfg.root.entrySet.asScala.foldLeft(Set[(String, AnyRef)]())((b, a) => {
      b + ((a.getKey(), a.getValue().unwrapped()))
    })
  }

  def getOptions(): List[String] = {
    cfg.root.entrySet.asScala.foldLeft(List[String]())((opts, entry) => {
      entry.getKey() :: opts
    })
  }

  def getSection(section: String): Option[Configuration] = {
    if (cfg.hasPath(section)) Some(Configuration(cfg.getConfig(section)))
    else None
  }

  def getSections(): List[String] = {
    cfg.root.entrySet.asScala.foldLeft(List[String]())((b, a) => {
      val v = a.getValue()
      if (v.valueType != ConfigValueType.OBJECT) b
      else a.getKey() :: b
    })
  }

  def foldSections[B](z: B)(f: (B, (String, Configuration)) => B): B = {
    getSections.foldLeft(z)((zz, s) => {
      val section = getSection(s)
      f(zz, (s, section.get))
    })
  }

  // value gather
  def getValue[T](path: String)(gather: String => T): T = {
    gather(path)
  }

  def getValue[T](path: String, default: T)(gather: String => T): T = {
    try {
      gather(path)
    } catch {
      case m: ConfigException.Missing => default
      case e: Throwable => throw e
    }
  }

  def getValueOption[T](path: String)(gather: String => T): Option[T] = {
    try {
      Some(gather(path))
    } catch {
      case e: Throwable => None
    }
  }

  // map methods to config object
  def getString(path: String): String = getValue(path)(cfg.getString)
  def getString(path: String, default: String): String =
    getValue(path, default)(cfg.getString)
  def getStringOption(path: String): Option[String] =
    getValueOption(path)(cfg.getString)

  def getInt(path: String): Int = getValue(path)(cfg.getInt)
  def getInt(path: String, default: Int): Int =
    getValue(path, default)(cfg.getInt)
  def getIntOption(path: String): Option[Int] =
    getValueOption(path)(cfg.getInt)

  def getLong(path: String): Long = getValue(path)(cfg.getLong)
  def getLong(path: String, default: Long): Long =
    getValue(path, default)(cfg.getLong)
  def getLongOption(path: String): Option[Long] =
    getValueOption(path)(cfg.getLong)

  def getBytes(path: String): Long = getValue(path)(cfg.getBytes)
  def getBytes(path: String, default: Long): Long =
    getValue(path, default)(cfg.getBytes)
  def getBytesOption(path: String): Option[Long] =
    getValueOption(path)(cfg.getBytes)

  def getBoolean(path: String): Boolean = getValue(path)(cfg.getBoolean)
  def getBoolean(path: String, default: Boolean): Boolean =
    getValue(path, default)(cfg.getBoolean)
  def getBooleanOption(path: String): Option[Boolean] =
    getValueOption(path)(cfg.getBoolean)

  def getIntList(path: String): List[Int] =
    getValue(path)(p => { cfg.getIntList(p).asScala.map(_.toInt).toList })
  def getIntList(path: String, default: List[Int]): List[Int] =
    getValue(path, default)(p => { cfg.getIntList(p).asScala
                                     .map(_.toInt).toList })
  def getIntListOption(path: String): Option[List[Int]] =
    getValueOption(path)(p => { cfg.getIntList(p).asScala
                                  .map(_.toInt).toList })

  def getStringList(path: String): List[String] =
    getValue(path)(p => { cfg.getStringList(p).asScala.toList })
  def getStringList(path: String, default: List[String]): List[String] =
    getValue(path, default)(p => { cfg.getStringList(p).asScala.toList })
  def getStringListOption(path: String): Option[List[String]] =
    getValueOption(path)(p => { cfg.getStringList(p).asScala.toList })

  // see: https://github.com/scala/scala-java8-compat/issues/85
  def getDuration(path: String): FiniteDuration =
    getValue(path)(p => { Duration.fromNanos(cfg.getDuration(p).toNanos) })
  def getDuration(path: String, default: FiniteDuration): FiniteDuration =
    getValue(path, default)(p => {
      Duration.fromNanos(cfg.getDuration(p).toNanos) 
    })
  def getDurationOption(path: String): Option[FiniteDuration] =
    getValueOption(path)(p => {
      Duration.fromNanos(cfg.getDuration(p).toNanos)
    })

  def getJDuration(path: String): JDuration =
    getValue(path)(p => { cfg.getDuration(p) })
  def getJDuration(path: String, default: JDuration): JDuration =
    getValue(path, default)(p => { cfg.getDuration(p) })
  def getJDurationOption(path: String): Option[JDuration] =
    getValueOption(path)(p => { cfg.getDuration(p) })
}

object Configuration {
  def quotePath(path: String): String = s""""${path}""""
}
