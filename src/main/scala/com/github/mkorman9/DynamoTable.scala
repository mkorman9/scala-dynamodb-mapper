package com.github.mkorman9

import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.Select
import com.github.mkorman9.DynamoDSL.QueryParts
import com.github.mkorman9.exception._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Provides mapping for case class specified in parameter C
  *
  * @tparam C case class to map
  * @param nameInDatabase Name of table in database
  */
abstract class DynamoTable[C](nameInDatabase: String) extends DynamoDatabaseEntity(nameInDatabase) {
  type MappedAttributeValues = Map[String, (Option[Any], Boolean)]
  type AnyAttribute = DynamoAttribute[_, _]

  private var _cachedTableReference: Option[Table] = None

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
            case v => (item.name, item.convertToDatabaseReadableValue(v)) :: accumulated
          }
        }
      }
    }

    def mapHashKeyToDatabaseType(): Any = {
      getHashKey().convertToDatabaseReadableValue(findValueForField(getHashKey().name))
    }

    val table = findCachedTableObject(dynamoDB)
    val attributesListMappedToDatabaseTypes = mapAttributesToDatabaseTypes()
    val hashKeyValueMappedToDatabaseType = mapHashKeyToDatabaseType()
    val optionalSortKey = getSortKey()

    optionalSortKey match {
      case Some(sortKey) => {
        val sortKeyMappedToDatabaseTypes = sortKey.convertToDatabaseReadableValue(findValueForField(sortKey.name))

        table.put(
          hashKeyValueMappedToDatabaseType,
          sortKeyMappedToDatabaseTypes,
          attributesListMappedToDatabaseTypes: _*
        )
      }
      case None => {
        table.put(
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
    val item = findCachedTableObject(dynamoDB).get(hashPK)
    item.map(_ =>
      mapQueryResultToCaseClass(
        hashKey = getHashKey(),
        sortKey = getSortKey(),
        nonKeyAttributes = getNonKeyAttributes(),
        queryResult = item.get,
        dynamoDB = dynamoDB,
        c = c
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
    val item = findCachedTableObject(dynamoDB).get(hashPK, sortPK)
    item.map(_ =>
      mapQueryResultToCaseClass(
        hashKey = getHashKey(),
        sortKey = getSortKey(),
        nonKeyAttributes = getNonKeyAttributes(),
        queryResult = item.get,
        dynamoDB = dynamoDB,
        c = c
      )
    )
  }

  /**
    * Returns a result of the database query built from specified keyConditions
    *
    * @param queryParts Sequence of conditions to build the query. Sequence must contain a reference to the hash key
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def query(queryParts: QueryParts)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    mapQueryResultsListToCaseClass(
      hashKey = getHashKey(),
      sortKey = getSortKey(),
      nonKeyAttributes = getNonKeyAttributes(),
      queryResult = findCachedTableObject(dynamoDB).query(queryParts),
      dynamoDB = dynamoDB,
      c = c
    )
  }

  /**
    * Returns a result of the database query built from specified keyConditions. Query is performed in secondary index specified as parameter
    *
    * @param index Secondary index to perform query on
    * @param queryParts Sequence of conditions to build the query. Sequence must contain a reference to the hash key
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def query(index: DynamoSecondaryIndex[_], queryParts: QueryParts)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    def retrieveLocalSecondaryIndex(table: Table): LocalSecondaryIndex = {
      val indexesList = dynamoDB.describe(table).get.getLocalSecondaryIndexes
      if (indexesList == null)
        throw new SecondaryIndexNotFoundException("No local secondary indexes was found in table")

      val indexPotentiallyFoundInDatabase = indexesList.asScala.find(_.getIndexName == index._nameInDatabase)
      indexPotentiallyFoundInDatabase match {
        case None => throw new SecondaryIndexNotFoundException("Local secondary index with specified name was not found in table")
        case Some(indexFoundInDatabase) => LocalSecondaryIndex(indexFoundInDatabase)
      }
    }

    def retrieveGlobalSecondaryIndex(): GlobalSecondaryIndex = {
      val indexesList = dynamoDB.describeTable(_nameInDatabase).getTable.getGlobalSecondaryIndexes // describeTable() because awscala does not directly support global indexes
      if (indexesList == null)
        throw new SecondaryIndexNotFoundException("No global secondary indexes was found in table")

      val indexPotentiallyFoundInDatabase = indexesList.asScala.find(_.getIndexName == index._nameInDatabase)
      indexPotentiallyFoundInDatabase match {
        case None => throw new SecondaryIndexNotFoundException("Global secondary index with specified name was not found in table")
        case Some(indexFoundInDatabase) => GlobalSecondaryIndex(indexFoundInDatabase)
      }
    }

    val table = findCachedTableObject(dynamoDB)
    val allAttributes = getAllAttributes()
    val indexHashKey = allAttributes.find(_.name == index.getHashKey().name).get
    val indexSortKey = index.getSortKey().flatMap(sortKey => allAttributes.find(_.name == sortKey.name))

    val secondaryIndex = index._indexType match {
      case DynamoLocalSecondaryIndex => retrieveLocalSecondaryIndex(table)
      case DynamoGlobalSecondaryIndex => retrieveGlobalSecondaryIndex()
    }

    val nonKeyAttributes = allAttributes filter { a => a != indexHashKey && a != secondaryIndex }
    mapQueryResultsListToCaseClass(
      hashKey = indexHashKey,
      sortKey = indexSortKey,
      nonKeyAttributes = nonKeyAttributes,
      queryResult = findCachedTableObject(dynamoDB).queryWithIndex(secondaryIndex, queryParts),
      dynamoDB = dynamoDB,
      c = c
    )
  }

  /**
    * Returns a result of the database table scan built from specified keyConditions
    *
    * @param queryParts Sequence of conditions to build the query
    * @param limit Maximum counts of items returned
    * @param segment Number of segment to retrieve
    * @param totalSegments Total number of segments
    * @param dynamoDB Connection to database
    * @param c case class ClassTag
    * @throws HashKeyNotFoundException When hash key is not returned in query result
    * @throws SortKeyNotFoundException When sort key is defined but not returned in query result
    * @throws AttributeNotFoundException When attribute is marked as required but not returned in query result
    */
  def scan(queryParts: QueryParts, limit: Int = 1000, segment: Int = 0, totalSegments: Int = 1)(implicit dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    def performTableScan(): Seq[Item] = {
      findCachedTableObject(dynamoDB).scan(
        filter = queryParts,
        select = Select.ALL_ATTRIBUTES,
        attributesToGet = Nil,
        limit = limit,
        segment = segment,
        totalSegments = totalSegments
      )
    }

    mapQueryResultsListToCaseClass(
      hashKey = getHashKey(),
      sortKey = getSortKey(),
      nonKeyAttributes = getNonKeyAttributes(),
      queryResult = performTableScan(),
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
      table = findCachedTableObject(dynamoDB),
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
      table = findCachedTableObject(dynamoDB),
      hashPK = hashKey
    )
  }

  private def findCachedTableObject(dynamoDB: DynamoDB): Table = {
    def resolveTable(): Table = {
      dynamoDB.table(_nameInDatabase) match {
        case Some(table) => table
        case None => throw new TableNotFoundException("Specified table was not found")
      }
    }

    this.synchronized {
      _cachedTableReference match {
        case None => this._cachedTableReference = Some(resolveTable()); this._cachedTableReference.get
        case Some(table) => table
      }
    }
  }

  private def mapQueryResultToCaseClass(hashKey: AnyAttribute, sortKey: Option[AnyAttribute], nonKeyAttributes: Seq[AnyAttribute],
                                   queryResult: Item, dynamoDB: DynamoDB, c: ClassTag[C]): C = {
    createCaseClass(
      mapDatabaseItemToRealValues(
        hashKey = hashKey,
        sortKey = sortKey,
        nonKeyAttributes = nonKeyAttributes,
        item = queryResult
      ),
      c = c
    )
  }

  private def mapQueryResultsListToCaseClass(hashKey: AnyAttribute, sortKey: Option[AnyAttribute],  nonKeyAttributes: Seq[AnyAttribute],
                                     queryResult: Seq[Item], dynamoDB: DynamoDB, c: ClassTag[C]): Seq[C] = {
    queryResult
      .map(item =>
        mapDatabaseItemToRealValues(
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

  private def mapDatabaseItemToRealValues(hashKey: AnyAttribute, sortKey: Option[AnyAttribute],
                                  nonKeyAttributes: Seq[AnyAttribute], item: Item): MappedAttributeValues = {
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

    hashKey.retrieveValueFromItem(item) match {
      case None => throw new HashKeyNotFoundException("Hash key not retrieved from database")
      case Some(_) => {
        val mappedHashKey: Option[Any] = resolveHashKey
        val mappedSortKey: Option[Any] = sortKey.map(resolveSortKey)
        val keys: MappedAttributeValues = createMapFromKeys(mappedHashKey, mappedSortKey)

        keys ++ resolveNonKeyAttributes
      }
    }
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
