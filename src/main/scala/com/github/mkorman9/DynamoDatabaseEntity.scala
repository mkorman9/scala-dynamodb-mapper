package com.github.mkorman9

abstract class DynamoDatabaseEntity(nameInDatabase: String) {
  val _nameInDatabase = nameInDatabase

  val _keys: (DynamoAttribute[_], DynamoAttribute[_])

  val _nonKeyAttributes: Seq[DynamoAttribute[_]]

  def getHashKey: DynamoAttribute[_] = _keys._1

  def getSortKey: Option[DynamoAttribute[_]] = if (_keys._2 != DynamoEmptyAttribute) Some(_keys._2) else None

  def getKeyAttributes = List(getHashKey) ++ getSortKey.toList

  def getNonKeyAttributes = _nonKeyAttributes

  def getAllAttributes = getKeyAttributes ++ getNonKeyAttributes
}
