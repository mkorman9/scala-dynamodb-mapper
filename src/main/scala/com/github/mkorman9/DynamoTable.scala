package com.github.mkorman9

import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.{Select, Condition}
import com.github.mkorman9.exception._

import scala.reflect.ClassTag
import collection.JavaConverters._

/**
  * Provides mapping for case class specified in parameter C
  *
  * @tparam C case class to map
  * @param nameInDatabase Name of table in database
  */
abstract class DynamoTable[C](nameInDatabase: String) extends DynamoDatabaseEntity(nameInDatabase) {
  type KeyConditions = Seq[(String, Condition)]
  type MappedAttributeValues = Map[String, (Option[Any], Boolean)]
  type AnyAttribute = DynamoAttribute[_, _]

  /**
    * Saves specified value in the database. If item specified by hash key and sort key already exists in database, it will be overwritten
    *
    * @param value Object of case class to save
    * @param dynamoDB Connection to database
    */
  def put(value: C)(implicit dynamoDB: DynamoDB): Unit = {
    def findValueForField(fieldName: String): Any = {
      val field = value.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(value)
    }

    def mapAttributesToDatabaseTypes(): List[(String, Any)] = {
      getNonKeyAttributes().foldLeft(List[(String, Any)]()) {
        (accumulated, item) => {
          findValueForField(item.name) match {
            case None => accumulated
            case Some(v) => (item.name, item.convertToDatabaseReadableValue(v)) :: accumulated
            case _ => (item.name, item.convertToDatabaseReadableValue(findValueForField(item.name))) :: accumulated
          }
        }
      }
    }

    def mapHashKeyToDatabaseType(): Any = {
      getHashKey().convertToDatabaseReadableValue(findValueForField(getHashKey().name))
    }

    val attributesListMappedToDatabaseTypes = mapAttributesToDatabaseTypes()
    val hashKeyValueMappedToDatabaseType = mapHashKeyToDatabaseType()
    val optionalSortKey = getSortKey()

    optionalSortKey match {
      case Some(sortKey) => {
        val sortKeyMappedToDatabaseTypes = sortKey.convertToDatabaseReadableValue(findValueForField(sortKey.name))

        findTable(dynamoDB).put(
          hashKeyValueMappedToDatabaseType,
          sortKeyMappedToDatabaseTypes,
          attributesListMappedToDatabaseTypes: _*
        )
      }
      case None => {
        findTable(dynamoDB).put(
          hashKeyValueMappedToDatabaseType,
          attributesListMappedToDatabaseTypes: _*
        )
      }
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
    item.flatMap(_ => Some(
        mapQuerySingleResult(
          hashKey = getHashKey(),
          sortKey = getSortKey(),
          nonKeyAttributes = getNonKeyAttributes(),
          queryResult = item.get,
          dynamoDB = dynamoDB,
          c = c
        )
      )
    )
  }

  /**
    * Returns a single item from database by its hash key and sort key
    *
    * @param hashPK Value of hash key
    * @return Item or None if it was not found
    */
  def get(hashPK: Any, sortPK: Any)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Option[C] = {
    val item = findTable(dynamoDB).get(hashPK, sortPK)
    item.flatMap(_ => Some(
        mapQuerySingleResult(
          hashKey = getHashKey(),
          sortKey = getSortKey(),
          nonKeyAttributes = getNonKeyAttributes(),
          queryResult = item.get,
          dynamoDB = dynamoDB,
          c = c
        )
      )
    )
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
  def query(keyConditions: KeyConditions)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    mapQueryResultSequence(
      hashKey = getHashKey(),
      sortKey = getSortKey(),
      nonKeyAttributes = getNonKeyAttributes(),
      queryResult = findTable(dynamoDB).query(keyConditions),
      dynamoDB = dynamoDB,
      c = c
    )
  }

  /**
    * Returns a result of the database query built from specified keyConditions. Query is performed in secondary index specified as parameter
    *
    * @param index Secondary index to perform query on
    * @param keyConditions Sequence of conditions to build the query. Sequence must contain a reference to the hash key
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def query(index: DynamoSecondaryIndex[_], keyConditions: KeyConditions)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    def retrieveLocalSecondaryIndex(table: Table): LocalSecondaryIndex = {
      val indexesList = dynamoDB.describe(table).get.getLocalSecondaryIndexes
      if (indexesList == null)
        throw new SecondaryIndexNotFoundException("No local secondary indexes was found in table")

      val indexFoundInDatabaseOption = indexesList.asScala.find(_.getIndexName == index._nameInDatabase)
      if (indexFoundInDatabaseOption.isEmpty)
        throw new SecondaryIndexNotFoundException("Local secondary index with specified name was not found in table")

      LocalSecondaryIndex(indexFoundInDatabaseOption.get)
    }

    def retrieveGlobalIndex(): GlobalSecondaryIndex = {
      val indexesList = dynamoDB.describeTable(_nameInDatabase).getTable.getGlobalSecondaryIndexes // describeTable() because awscala does not directly support global indexes
      if (indexesList == null)
        throw new SecondaryIndexNotFoundException("No global secondary indexes was found in table")

      val indexFoundInDatabaseOption = indexesList.asScala.find(_.getIndexName == index._nameInDatabase)
      if (indexFoundInDatabaseOption.isEmpty)
        throw new SecondaryIndexNotFoundException("Global secondary index with specified name was not found in table")

      GlobalSecondaryIndex(indexFoundInDatabaseOption.get)
    }

    val table = findTable(dynamoDB)
    val allAttributes = getAllAttributes()
    val indexHashKey = allAttributes.find(_.name == index.getHashKey().name).get
    val indexSortKey = index.getSortKey().flatMap(sortKey => allAttributes.find(_.name == sortKey.name))

    val secondaryIndex = index._indexType match {
      case DynamoLocalSecondaryIndex => retrieveLocalSecondaryIndex(table)
      case DynamoGlobalSecondaryIndex => retrieveGlobalIndex()
    }

    val nonKeyAttributes = allAttributes filter { a => a != indexHashKey && a != secondaryIndex }
    mapQueryResultSequence(
      hashKey = indexHashKey,
      sortKey = indexSortKey,
      nonKeyAttributes = nonKeyAttributes,
      queryResult = findTable(dynamoDB).queryWithIndex(secondaryIndex, keyConditions),
      dynamoDB = dynamoDB,
      c = c
    )
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
  def scan(keyConditions: KeyConditions, limit: Int = 1000, segment: Int = 0, totalSegments: Int = 1)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    def performTableScan = {
      findTable(dynamoDB).scan(
        filter = keyConditions,
        select = Select.ALL_ATTRIBUTES,
        attributesToGet = Nil,
        limit = limit,
        segment = segment,
        totalSegments = totalSegments
      )
    }

    mapQueryResultSequence(
      hashKey = getHashKey(),
      sortKey = getSortKey(),
      nonKeyAttributes = getNonKeyAttributes(),
      queryResult = performTableScan,
      dynamoDB = dynamoDB,
      c = c
    )
  }

  /**
    * Deletes from database item specified by hashKey and sortKey
    *
    * @param hashKey Hash key of item to delete
    * @param sortKey Sort key of item to delete
    * @param dynamoDB Connection to database
    */
  def delete(hashKey: Any, sortKey: Any)(implicit dynamoDB: DynamoDB): Unit = {
    dynamoDB.deleteItem(
      table = findTable(dynamoDB),
      hashPK = hashKey,
      rangePK = sortKey
    )
  }

  /**
    * Deletes from database item specified by hashKey
    *
    * @param hashKey Hash key of item to delete
    * @param dynamoDB Connection to database
    */
  def delete(hashKey: Any)(implicit dynamoDB: DynamoDB): Unit = {
    dynamoDB.deleteItem(
      table = findTable(dynamoDB),
      hashPK = hashKey
    )
  }

  private def findTable(dynamoDB: DynamoDB): Table = {
    dynamoDB.table(_nameInDatabase) match {
      case Some(table) => table
      case None => throw new TableNotFoundException("Specified table was not found")
    }
  }

  private def mapQuerySingleResult(hashKey: AnyAttribute, sortKey: Option[AnyAttribute], nonKeyAttributes: Seq[AnyAttribute],
                                   queryResult: Item, dynamoDB: DynamoDB, c: ClassTag[C]): C = {
    createCaseClass(
      mapItem(
        hashKey = hashKey,
        sortKey = sortKey,
        nonKeyAttributes = nonKeyAttributes,
        item = queryResult
      ),
      c = c
    )
  }

  private def mapQueryResultSequence(hashKey: AnyAttribute, sortKey: Option[AnyAttribute],  nonKeyAttributes: Seq[AnyAttribute],
                                     queryResult: Seq[Item], dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    queryResult
      .map(item =>
        mapItem(
          hashKey = hashKey,
          sortKey = sortKey,
          nonKeyAttributes = nonKeyAttributes,
          item = item
        )
      )
      .map(v =>
        createCaseClass(v, c)
      )
  }

  private def mapItem(hashKey: AnyAttribute, sortKey: Option[AnyAttribute], nonKeyAttributes: Seq[AnyAttribute], item: Item): MappedAttributeValues = {
    def resolveHashKey: Option[Any] = {
      val hashKeyItemValue = hashKey.retrieveValueFromItem(item).get
      Some(hashKey.convertToRealValue(hashKeyItemValue))
    }

    def resolveSortKey(key: AnyAttribute): Any = {
      val sortKeyValue = key.retrieveValueFromItem(item)
      if (sortKeyValue.isEmpty)
          throw new SortKeyNotFoundException("Sort key not retrieved from database")

      key.convertToRealValue(sortKeyValue.get)
    }

    def createMapFromKeys(mappedHashKey: Option[Any], mappedSortKey: Option[Any]): MappedAttributeValues = {
      val mappedHashKeyWithRequire = Map(hashKey.name -> (mappedHashKey, true))
      val mappedSortKeyWithRequire =
        if (mappedSortKey.isDefined)
          Map(sortKey.get.name -> (mappedSortKey, true))
        else
          Map()

      mappedHashKeyWithRequire ++ mappedSortKeyWithRequire
    }

    def resolveNonKeyAttributes(): MappedAttributeValues = {
      val mappedAttributes = nonKeyAttributes map { v =>
        val attributeValueOption = v.retrieveValueFromItem(item)
        if (attributeValueOption.isDefined) {
          val attributeRealValue = v.convertToRealValue(attributeValueOption.get)

          v.name -> (Some(attributeRealValue), v.requiredValue)
        }
        else {
          if (v.requiredValue)
            throw new AttributeNotFoundException("Required attribute '" + v.name + "' not retrieved from database")

          v.name -> (None, false)
        }
      }

      mappedAttributes.toMap
    }

    if (hashKey.retrieveValueFromItem(item).isEmpty)
      throw new HashKeyNotFoundException("Hash key not retrieved from database")

    val mappedHashKey: Option[Any] = resolveHashKey
    val mappedSortKey: Option[Any] = sortKey map resolveSortKey
    val keys: MappedAttributeValues = createMapFromKeys(mappedHashKey, mappedSortKey)

    keys ++ resolveNonKeyAttributes
  }

  private def createCaseClass(vals: MappedAttributeValues, c: ClassTag[C]) = {
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
