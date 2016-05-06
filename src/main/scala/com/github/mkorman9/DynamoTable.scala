package com.github.mkorman9

import awscala.dynamodbv2.DynamoDB
import com.amazonaws.services.dynamodbv2.model.Condition
import ovh.mihau.dynamodb.exception.AttributeNotFoundException

import scala.reflect.ClassTag

abstract class DynamoTable {
  val name: String
  val hashKey: DynamoAttribute[_]
  val sortKey: DynamoAttribute[_] = DynamoEmptyAttribute
  val attr: Seq[DynamoAttribute[_]]

  def put(value: Any)(implicit dynamoDB: DynamoDB): Unit = {
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

  def query[C <: AnyRef](keyConditions: Seq[(String, Condition)])(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
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
          v.name ->(Some(v.convertToRealValue(value.get)), v.requiredValue)
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
}
