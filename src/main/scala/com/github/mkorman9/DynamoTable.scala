package com.github.mkorman9

import awscala.dynamodbv2.{Table, Item, DynamoDB}
import com.amazonaws.services.dynamodbv2.model.{Select, Condition}
import com.github.mkorman9.exception.{TableNotFoundException, SortKeyNotFoundException, HashKeyNotFoundException, AttributeNotFoundException}

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
    * Returns a single item from database by its hash key
    *
    * @param hashPK Value of hash key
    * @return Item or None if it was not found
    */
  def get(hashPK: Any)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Option[C] = {
    val item = findTable(dynamoDB).get(hashPK)
    if (item.isEmpty) None else Some(mapQuerySingleResult(item.get, dynamoDB, c))
  }

  /**
    * Returns a single item from database by its hash key and sort key
    *
    * @param hashPK Value of hash key
    * @return Item or None if it was not found
    */
  def get(hashPK: Any, sortPK: Any)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Option[C] = {
    val item = findTable(dynamoDB).get(hashPK, sortPK)
    if (item.isEmpty) None else Some(mapQuerySingleResult(item.get, dynamoDB, c))
  }

  /**
    * Returns a result of the database query built from specified keyConditions
    *
    * @param keyConditions Sequence of conditions to build the query. Sequence must contain a reference to the hash key
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def query(keyConditions: Seq[(String, Condition)])(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    mapQueryResultSequence(findTable(dynamoDB).query(keyConditions), dynamoDB, c)
  }

  /**
    * Returns a result of the database table scan built from specified keyConditions
    *
    * @param keyConditions Sequence of conditions to build the query
    * @param limit Maximum counts of items returned
    * @param segment Number of segment to retrieve
    * @param totalSegments Total number of segments
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def scan(keyConditions: Seq[(String, Condition)], limit: Int = 1000, segment: Int = 0, totalSegments: Int = 1)
          (implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    mapQueryResultSequence(findTable(dynamoDB).scan(keyConditions, Select.ALL_ATTRIBUTES, Nil, limit, segment, totalSegments), dynamoDB, c)
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

  private def findTable(dynamoDB: DynamoDB): Table = {
    val tab = dynamoDB.table(name)
    if (tab.isEmpty) throw new TableNotFoundException("Specified table was not found")
    tab.get
  }

  private def mapQuerySingleResult(queryResult: Item, dynamoDB: DynamoDB, c: ClassTag[C]): C = {
    createCaseClass(mapItem(queryResult), c)
  }

  private def mapQueryResultSequence(queryResult: Seq[Item], dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    (queryResult map { mapItem }) map { v => createCaseClass(v, c) }
  }

  private def mapItem(item: Item): Map[String, (Option[Any], Boolean)] = {
    val returnedHashKey = hashKey.retrieveValueFromItem(item)
    if (returnedHashKey.isEmpty) throw new HashKeyNotFoundException("Hash key not retrieved from database")
    val hashKeyValue = Some(hashKey.convertToRealValue(returnedHashKey.get))
    val keys =
      if (sortKey == DynamoEmptyAttribute)
        Map(hashKey.name -> (hashKeyValue, true), sortKey.name -> (None, true))
      else {
        val returnedSortKey = sortKey.retrieveValueFromItem(item)
        if (returnedSortKey.isEmpty) throw new SortKeyNotFoundException("Sort key not retrieved from database")
        Map(hashKey.name -> (hashKeyValue, true), sortKey.name ->
          (Some(sortKey.convertToRealValue(returnedSortKey.get)), true))
      }

    keys ++ (attr map { v =>
      val value = v.retrieveValueFromItem(item)
      if (value.isDefined)
        v.name -> (Some(v.convertToRealValue(value .get)), v.requiredValue)
      else {
        if (v.requiredValue) {
          val name = v.name
          throw new AttributeNotFoundException(s"Required attribute '$name' not retrieved from database")
        }
        v.name -> (None, false)
      }
    })
  }

  private def createCaseClass(vals: Map[String, (Option[Any], Boolean)], c: ClassTag[C]) = {
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
}
