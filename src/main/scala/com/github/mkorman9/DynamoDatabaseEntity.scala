package com.github.mkorman9

abstract class DynamoDatabaseEntity(nameInDatabase: String) {
  val _nameInDatabase = nameInDatabase

  val _keys: (DynamoAttribute[_], DynamoAttribute[_])

  def getHashKey: DynamoAttribute[_] = _keys._1

  def getSortKey: Option[DynamoAttribute[_]] = if (_keys._2 != DynamoEmptyAttribute) Some(_keys._2) else None

  def getKeyAttributes = List(getHashKey) ++ getSortKey.toList

  def getNonKeyAttributes = getAllAttributes filter { f => !getKeyAttributes.contains(f) }

  def getAllAttributes = this.getClass.getDeclaredMethods filter { m =>
    !m.getName.startsWith("_")
  } map { m =>
    m.invoke(this, Nil : _*).asInstanceOf[DynamoAttribute[_]]
  }
}
