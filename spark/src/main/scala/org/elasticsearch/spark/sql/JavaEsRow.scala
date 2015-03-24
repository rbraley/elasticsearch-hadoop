package org.elasticsearch.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Row => ScalaRow}

private[spark] class JavaEsRow(private[spark] val esrow: ScalaEsRow) extends Row {
  def apply(i: Int): Any = esrow.apply(i)
  def copy(): org.apache.spark.sql.Row = esrow.copy()
  def getBoolean(i: Int): Boolean = esrow.getBoolean(i)
  def getByte(i: Int): Byte = esrow.getByte(i)
  def getDouble(i: Int): Double = esrow.getDouble(i)
  def getFloat(i: Int): Float = esrow.getFloat(i)
  def getInt(i: Int): Int = esrow.getInt(i)
  def getLong(i: Int): Long = esrow.getLong(i)
  def getShort(i: Int): Short = esrow.getShort(i)
  def getString(i: Int): String = esrow.getString(i)
  def isNullAt(i: Int): Boolean = esrow.isNullAt(i)
  def length: Int = esrow.length
  def toSeq: Seq[Any] = esrow.values.toSeq

}