package com.github.mkorman9

import java.nio.ByteBuffer

import awscala.dynamodbv2.Item
import org.joda.time.DateTime

import scala.collection.JavaConverters._

/**
  * Representation of single attribute of database item
  * May be extended to provide mapping for user-defined attribute types
  *
  * @tparam T Type in which value will be stored in database
  */
trait DynamoAttribute[T] extends DynamoGeneralOperators {
  /**
    * Name of attribute. Must match the name of case class member!
    */
  val name: String

  /**
    * Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
    */
  val requiredValue: Boolean

  /**
    * Finds value of attribute in awscala.dynamodbv2.Item object
    *
    * @param item Object to find value in
    * @return Value of attribute or None if it is not found
    */
  def retrieveValueFromItem(item: Item): Option[T]

  /**
    * Converts between value used in case class and value which is internally stored in database
    * For some objects it's impossible to provide direct conversion. For example for Joda's DateTime, value is internally stored as number
    * Should be a mirror for convertToRealValue
    *
    * @param value Value of attribute used in case class
    * @return Value of attribute as database internal value
    */
  def convertToDatebaseReadableValue(value: Any): Any

  /**
    * Converts between value which is internally stored in database and value used in case class
    * For some objects it's impossible to provide direct conversion. For example for Joda's DateTime, value is internally stored as number
    * Should be a mirror for convertToDatebaseReadableValue
    *
    * @param value Value of attribute as database internal value
    * @return Value of attribute used in case class
    */
  def convertToRealValue(value: Any): Any
}

/**
  * Defined only to provide info about empty sort key. Should not be used anywhere else
  */
object DynamoEmptyAttribute extends DynamoAttribute[Any] {
  override val name: String = ""

  override val requiredValue: Boolean = true

  override def retrieveValueFromItem(item: Item): Option[Any] = None

  override def convertToRealValue(value: Any): Any = None

  override def convertToDatebaseReadableValue(value: Any): Any = None
}

/**
  * Provides conversion for Int value. Int is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for Long value. Long is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for String value. String is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for Boolean value. Int is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for sequence of strings. Sequence of strings is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for sequence of ints. Sequence of ints is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for sequence of longs. Sequence of longs is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for java's ByteBuffer objects. ByteBuffer is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for sequence of java's ByteBuffer objects. Sequence of ByteBuffers is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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

/**
  * Provides conversion for sequence of Joda's DateTime objects. DateTime is stored as Long (number of milliseconds since 1 January 1970)
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Optional[_]
  */
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
