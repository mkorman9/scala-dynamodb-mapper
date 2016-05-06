package com.github.mkorman9

import java.nio.ByteBuffer

import awscala.dynamodbv2.Item
import org.joda.time.DateTime

import scala.collection.JavaConverters._

trait DynamoAttribute[T] {
  val name: String
  val requiredValue: Boolean
  def retrieveValueFromItem(item: Item): Option[T]
  def convertToDatebaseReadableValue(value: Any): Any
  def convertToRealValue(value: Any): Any
}

object DynamoEmptyAttribute extends DynamoAttribute[Any] {
  override val name: String = ""

  override val requiredValue: Boolean = true

  override def retrieveValueFromItem(item: Item): Option[Any] = None

  override def convertToRealValue(value: Any): Any = None

  override def convertToDatebaseReadableValue(value: Any): Any = None
}

case class DynamoInt(fieldName: String, required: Boolean = true) extends DynamoAttribute[Int] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Int] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getN.toInt)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value.toString.toInt
}

case class DynamoLong(fieldName: String, required: Boolean = true) extends DynamoAttribute[Long] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Long] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getN.toLong)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value.toString.toLong
}

case class DynamoString(fieldName: String, required: Boolean = true) extends DynamoAttribute[String] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[String] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getS)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value.toString
}

case class DynamoBoolean(fieldName: String, required: Boolean = true) extends DynamoAttribute[Boolean] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Boolean] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getBOOL)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value.toString.toBoolean
}

case class DynamoStringSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[String]] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Seq[String]] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getSS.asScala)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value
}

case class DynamoIntSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[Int]] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Seq[Int]] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getNS.asScala map (_.toInt))
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value
}

case class DynamoLongSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[Long]] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Seq[Long]] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getNS.asScala map (_.toLong))
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value
}

case class DynamoByteBuffer(fieldName: String, required: Boolean = true) extends DynamoAttribute[ByteBuffer] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[ByteBuffer] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getB)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value
}

case class DynamoByteBufferSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[ByteBuffer]] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Seq[ByteBuffer]] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getBS.asScala)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Any = value
}

case class DynamoDateTime(fieldName: String, required: Boolean = true) extends DynamoAttribute[Long] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Long] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getN.toLong)
    else None
  }

  override def convertToDatebaseReadableValue(value: Any): Any = value.asInstanceOf[DateTime].getMillis

  override def convertToRealValue(value: Any): Any = new DateTime(value.asInstanceOf[Long])
}
