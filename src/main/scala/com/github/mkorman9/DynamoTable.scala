package com.github.mkorman9

import awscala.dynamodbv2.DynamoDB
import com.amazonaws.services.dynamodbv2.model.Condition
import com.github.mkorman9.exception.AttributeNotFoundException

import scala.reflect.ClassTag

/**
  * Provides mapping for case class specified in parameter C
  *
  * @tparam C case class to map
  */
abstract class DynamoTable[C] {
  /**
    * Name of table int the database
    */
  val name: String

  /**
    * Attribute chosen to be a hash key
    */
  val hashKey: DynamoAttribute[_]

  /**
    * Attribute chosen to be a sort key. Sort key is not required in table
    */
  val sortKey: DynamoAttribute[_] = DynamoEmptyAttribute

  /**
    * List of non-key attributes to map
    */
  val attr: Seq[DynamoAttribute[_]]

  /**
    * Saves specified value in the database. If item specified by hash key and sort key already exists in database, it will be overwritten
    *
    * @param value Object of case class to save
    * @param dynamoDB Connection to database
    */
  def put(value: C)(implicit dynamoDB: DynamoDB): Unit = {
    def findValueFor(name: String) = {
      val f = value.getClass.getDeclaredField(name)
      f.setAccessible(true)
      f.get(value)
    }
    val a = attr.foldLeft(List[(String, Any)]()) {
      (acc, item) => {
        val value = findValueFor(item.name)
        value match {
          case None => acc
          case Some(v) => (item.name, item.convertToDatebaseReadableValue(v)) :: acc
          case _ => (item.name, item.convertToDatebaseReadableValue(value)) :: acc
        }
      }
    }
    val hashKeyValue = hashKey.convertToDatebaseReadableValue(findValueFor(hashKey.name))
    if (sortKey == DynamoEmptyAttribute) {
      dynamoDB.table(name).get.put(hashKeyValue, a: _*)
    }
    else {
      val sortKeyValue = sortKey.convertToDatebaseReadableValue(findValueFor(sortKey.name))
      dynamoDB.table(name).get.put(hashKeyValue, sortKeyValue, a: _*)
    }
  }

  /**
    * Saves all of the elements specified in value in database
    *
    * @param values Sequence of case class objects to save
    * @param dynamoDB Connection to database
    */
  def putAll(values: Seq[C])(implicit dynamoDB: DynamoDB): Unit = {
    values foreach put
  }

  /**
    * Returns a result of the database query built from specified keyConditions
    *
    * @param keyConditions Sequence of conditions to build the query. Sequence must contain a reference to the hash key
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    */
  def query(keyConditions: Seq[(String, Condition)])(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    def createCaseClass(vals: Map[String, (Option[Any], Boolean)]) = {
      val ctor = c.runtimeClass.getConstructors.head
      val args = c.runtimeClass.getDeclaredFields.map(f => {
        val (value, required) = vals(f.getName)
        if (required) {
          value.get.asInstanceOf[Object]
        }
        else
          value.asInstanceOf[Object]
      })
      ctor.newInstance(args: _*).asInstanceOf[C]
    }
    val resp: Seq[Map[String, (Option[Any], Boolean)]] = dynamoDB.table(name).get.query(keyConditions) map { item =>
      val hashKeyValue = Some(hashKey.convertToRealValue(hashKey.retrieveValueFromItem(item).get))
      val keys =
        if (sortKey == DynamoEmptyAttribute)
          Map(hashKey.name ->(hashKeyValue, true), sortKey.name ->(None, true))
        else
          Map(hashKey.name ->(hashKeyValue, true), sortKey.name ->
            (Some(sortKey.convertToRealValue(sortKey.retrieveValueFromItem(item).get)), true))

      keys ++ (attr map { v =>
        val value = v.retrieveValueFromItem(item)
        if (value.isDefined)
          v.name ->(Some(v.convertToRealValue(value .get)), v.requiredValue)
        else {
          if (v.requiredValue) {
            val name = v.name
            throw new AttributeNotFoundException(s"Required attribute '$name' not retrieved from database")
          }
          v.name -> (None, false)
        }
      })
    }

    resp map (v => createCaseClass(v))
  }

  /**
    * Deletes from database item specified by hashKey and sortKey
    *
    * @param hashKey Hash key of item to delete
    * @param sortKey Sort key of item to delete
    * @param dynamoDB Connection to database
    */
  def delete(hashKey: Any, sortKey: Any)(implicit dynamoDB: DynamoDB) = {
    dynamoDB.deleteItem(dynamoDB.table(name).get, hashKey, sortKey)
  }

  /**
    * Deletes from database item specified by hashKey
    *
    * @param hashKey Hash key of item to delete
    * @param dynamoDB Connection to database
    */
  def delete(hashKey: Any)(implicit dynamoDB: DynamoDB) = {
    dynamoDB.deleteItem(dynamoDB.table(name).get, hashKey)
  }
}
