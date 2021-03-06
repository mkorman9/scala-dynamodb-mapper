package com.github.mkorman9

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}

import awscala.dynamodbv2.{AttributeValue, Item}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.collection.JavaConverters._

/**
  * Representation of single attribute of database item
  * May be extended to provide mapping for user-defined attribute types
  *
  * @tparam Original Type utilised in business logic
  * @tparam Stored Type in which value will be stored in database
  */
trait DynamoAttribute[Original, Stored] extends DynamoGeneralOperators {
  /**
    * Name of attribute. Must match the name of case class member!
    */
  val name: String

  /**
    * Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
    */
  val requiredValue: Boolean

  /**
    * Finds value of attribute in awscala.dynamodbv2.Item object
    *
    * @param item Object to find value in
    * @return Value of attribute or None if it is not found
    */
  def retrieveValueFromItem(item: Item): Option[Stored] = {
    item.attributes
      .find(a => a.name == name)
      .map(attribute => extractValueFromAttributeValue(attribute.value))
  }

  /**
    * Extracts stored value from AttributeValue object
    *
    * @param attributeValue Object to extract from
    * @return Value of attribute or None if it is not found
    */

  def extractValueFromAttributeValue(attributeValue: AttributeValue): Stored

  /**
    * Converts between value used in case class and value which is internally stored in database
    * For some objects it's impossible to provide direct conversion. For example for LocalDateTime, value is internally stored as string
    * Should be a mirror for convertToRealValue
    *
    * @param value Value of attribute used in case class
    * @return Value of attribute as database internal value
    */
  def convertToDatabaseReadableValue(value: Any): Any

  /**
    * Converts between value which is internally stored in database and value used in case class
    * For some objects it's impossible to provide direct conversion. For example for Joda's DateTime, value is internally stored as number
    * Should be a mirror for convertToDatabaseReadableValue
    *
    * @param value Value of attribute as database internal value
    * @return Value of attribute used in case class
    */
  def convertToRealValue(value: Any): Original
}

/**
  * Defined only to provide info about empty sort key. Should not be used anywhere else
  */
object DynamoEmptyAttribute extends DynamoAttribute[Any, Any] {
  override val name: String = ""

  override val requiredValue: Boolean = true

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Any = None

  override def convertToRealValue(value: Any): Any = None

  override def convertToDatabaseReadableValue(value: Any): Any = None
}

/**
  * Provides conversion for Int value. Int is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoInt(fieldName: String, required: Boolean = true) extends DynamoAttribute[Int, Int] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Int = attributeValue.getN.toInt

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Int = value.toString.toInt
}

/**
  * Provides conversion for Float value. Float is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoFloat(fieldName: String, required: Boolean = true) extends DynamoAttribute[Float, Float] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Float = attributeValue.getN.toFloat

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Float = value.toString.toFloat
}

/**
  * Provides conversion for Double value. Double is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoDouble(fieldName: String, required: Boolean = true) extends DynamoAttribute[Double, Double] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Double = attributeValue.getN.toDouble

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Double = value.toString.toDouble
}

/**
  * Provides conversion for BigInt value. BigInt is mapped by corresponding Java type.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoBigInt(fieldName: String, required: Boolean = true) extends DynamoAttribute[BigInt, java.math.BigInteger] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): java.math.BigInteger = new java.math.BigInteger(attributeValue.getN)

  override def convertToDatabaseReadableValue(value: Any): Any = value.asInstanceOf[BigInt].bigInteger

  override def convertToRealValue(value: Any): BigInt = BigInt(value.toString)
}

/**
  * Provides conversion for BigDecimal value. BigDecimal is mapped by corresponding Java type.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoBigDecimal(fieldName: String, required: Boolean = true) extends DynamoAttribute[BigDecimal, java.math.BigDecimal] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): java.math.BigDecimal = new java.math.BigDecimal(attributeValue.getN)

  override def convertToDatabaseReadableValue(value: Any): Any = value.asInstanceOf[BigDecimal].bigDecimal

  override def convertToRealValue(value: Any): BigDecimal = BigDecimal(value.toString)
}

/**
  * Provides conversion for Long value. Long is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoLong(fieldName: String, required: Boolean = true) extends DynamoAttribute[Long, Long] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def retrieveValueFromItem(item: Item): Option[Long] = {
    val v = item.attributes.find(a => a.name == name)
    if (v.isDefined) Some(v.get.value.getN.toLong)
    else None
  }

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Long = attributeValue.getN.toLong

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Long = value.toString.toLong
}

/**
  * Provides conversion for String value. String is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoString(fieldName: String, required: Boolean = true) extends DynamoAttribute[String, String] with DynamoStringOperators with DynamoCollectionOperators {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): String = attributeValue.getS

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): String = value.toString
}

/**
  * Provides conversion for Boolean value. Int is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoBoolean(fieldName: String, required: Boolean = true) extends DynamoAttribute[Boolean, Boolean] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Boolean = attributeValue.getBOOL

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Boolean = value.toString.toBoolean
}

/**
  * Provides conversion for sequence of strings. Sequence of strings is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoStringSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[String], Seq[String]] with DynamoCollectionOperators {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Seq[String] = attributeValue.getSS.asScala

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Seq[String] = value.asInstanceOf[Seq[String]]
}

/**
  * Provides conversion for sequence of ints. Sequence of ints is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoIntSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[Int], Seq[Int]] with DynamoCollectionOperators {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Seq[Int] = attributeValue.getNS.asScala map (_.toInt)

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Seq[Int] = value.asInstanceOf[Seq[Int]]
}

/**
  * Provides conversion for sequence of longs. Sequence of longs is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoLongSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[Long], Seq[Long]] with DynamoCollectionOperators {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Seq[Long] = attributeValue.getNS.asScala map (_.toLong)

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Seq[Long] = value.asInstanceOf[Seq[Long]]
}

/**
  * Provides conversion for java's ByteBuffer objects. ByteBuffer is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoByteBuffer(fieldName: String, required: Boolean = true) extends DynamoAttribute[ByteBuffer, ByteBuffer] {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): ByteBuffer = attributeValue.getB

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): ByteBuffer = value.asInstanceOf[ByteBuffer]
}

/**
  * Provides conversion for sequence of java's ByteBuffer objects. Sequence of ByteBuffers is mapped directly.
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoByteBufferSeq(fieldName: String, required: Boolean = true) extends DynamoAttribute[Seq[ByteBuffer], Seq[ByteBuffer]] with DynamoCollectionOperators {
  override val name: String = fieldName

  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): Seq[ByteBuffer] = attributeValue.getBS.asScala

  override def convertToDatabaseReadableValue(value: Any): Any = value

  override def convertToRealValue(value: Any): Seq[ByteBuffer] = value.asInstanceOf[Seq[ByteBuffer]]
}

/**
  * Provides conversion for sequence of LocalDateTime objects. DateTime is stored as String
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoLocalDateTime(fieldName: String, required: Boolean = true) extends DynamoAttribute[LocalDateTime, String] {
  val DateTimeFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  override val name: String = fieldName
  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): String = attributeValue.getS

  override def convertToDatabaseReadableValue(value: Any): Any = DateTimeFormat.format(value.asInstanceOf[LocalDateTime])

  override def convertToRealValue(value: Any): LocalDateTime = LocalDateTime.parse(value.toString, DateTimeFormat)
}

/**
  * Provides conversion for sequence of ZonedDateTime objects. DateTime is stored as String
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  */
case class DynamoZonedDateTime(fieldName: String, required: Boolean = true) extends DynamoAttribute[ZonedDateTime, String] {
  val DateTimeFormat = DateTimeFormatter.ISO_ZONED_DATE_TIME

  override val name: String = fieldName
  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): String = attributeValue.getS

  override def convertToDatabaseReadableValue(value: Any): Any = DateTimeFormat.format(value.asInstanceOf[ZonedDateTime])

  override def convertToRealValue(value: Any): ZonedDateTime = ZonedDateTime.parse(value.toString, DateTimeFormat)
}

/**
  * Provides JSON conversion for any object
  *
  * @param fieldName Name of attribute. Must match the name of case class member!
  * @param required  Is value required to be returned in every query. Set to false only if corresponding case class member is of type Option[_]
  *
  */
case class DynamoJSON[T: Manifest](fieldName: String, required: Boolean = true) extends DynamoAttribute[T, String] {
  val ObjectMapper = new ObjectMapper() with ScalaObjectMapper
  ObjectMapper.registerModule(DefaultScalaModule)

  override val name: String = fieldName
  override val requiredValue: Boolean = required

  override def extractValueFromAttributeValue(attributeValue: AttributeValue): String = attributeValue.getS

  override def convertToDatabaseReadableValue(value: Any): Any = ObjectMapper.writeValueAsString(value)

  override def convertToRealValue(value: Any): T = createObjectFromJson(value.asInstanceOf[String])

  private def createObjectFromJson(json: String): T = ObjectMapper.readValue[T](json)
}
