/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

object ClassLoader {
  def loadClassInstanceByName[C](clsName: String): C = {
    val c = Class.forName(clsName).asInstanceOf[Class[C]]
    c.newInstance()
  }

  def loadClassInstanceByName[C](clsName: String, argTypeTuples: Seq[(AnyRef, Class[_])]): C = {
    val args = argTypeTuples.map(_._1)
    val classes = argTypeTuples.map(_._2)

    val c = Class.forName(clsName).asInstanceOf[Class[C]]
    val cons = c.getConstructor(classes: _*)

    cons.newInstance(args: _*)
  }
}
