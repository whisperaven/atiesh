/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

object ComponentLoader {
  def createInstanceFor[C](fqcn: String): C = {
    val c = Class.forName(fqcn).asInstanceOf[Class[C]]
    c.newInstance()
  }

  def createInstanceFor[C](fqcn: String, args: Seq[(Class[_], AnyRef)]): C = {
    val types = args.map(_._1)
    val values = args.map(_._2)

    val c = Class.forName(fqcn).asInstanceOf[Class[C]]
    c.getConstructor(types: _*).newInstance(values: _*)
  }
}
