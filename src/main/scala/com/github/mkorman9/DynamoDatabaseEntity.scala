package com.github.mkorman9

/**
  * Represents entity in database. Entity is an object containing keys and attributes. For example both table and indexes are entities.
  *
  * @param nameInDatabase Name of entity in database
  */
abstract class DynamoDatabaseEntity(nameInDatabase: String) {
  /**
    * Name of entity in database
    */
  val _nameInDatabase = nameInDatabase

  /**
    * Key attributes. First one is hash key and the second one is sort key. Sort key might be set to DynamoEmptyAttribute
    */
  val _keys: (DynamoAttribute[_, _], DynamoAttribute[_, _])

  /**
    * Hash key
    *
    * @return Hash key from _keys tuple
    */
  def getHashKey: DynamoAttribute[_, _] = _keys._1

  /**
    * Sort key
    *
    * @return Some(...) when sort key is present and None if second key is equal to DynamoEmptyAttribute
    */
  def getSortKey: Option[DynamoAttribute[_, _]] =
    if (_keys._2 != DynamoEmptyAttribute)
      Some(_keys._2)
    else
      None

  /**
    * List of key attributes
    *
    * @return List of key attributes build from _keys tuple
    */
  def getKeyAttributes = getHashKey :: getSortKey.toList

  /**
    * List of non-key attributes
    *
    * @return List of all attributes without key attributes
    */
  def getNonKeyAttributes = getAllAttributes filter { f => !getKeyAttributes.contains(f) }

  /**
    * List of all attributes
    *
    * @return List of both key and non-key attributes
    */
  def getAllAttributes = this.getClass.getDeclaredMethods filter { m =>
    !m.getName.startsWith("_")
  } map { m =>
    m.invoke(this, Nil : _*).asInstanceOf[DynamoAttribute[_, _]]
  }
}
