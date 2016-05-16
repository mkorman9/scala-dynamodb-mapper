package com.github.mkorman9

/**
  * Represents secondary index of DynamoDB table
  *
  * @param nameInDatabase Name of table in database
  * @param _indexType Type of index, local or global
  * @param _sourceTable Table which is index using
  */
abstract class DynamoSecondaryIndex[T <: DynamoTable[_]](nameInDatabase: String,
                                    val _indexType: DynamoSecondaryIndexType,
                                    val _sourceTable: T) extends DynamoDatabaseEntity(nameInDatabase) {
}

/**
  * Represents type of secondary index of DynamoDB table
  */
sealed trait DynamoSecondaryIndexType

/**
  * Local secondary index type
  */
case object DynamoLocalSecondaryIndex extends DynamoSecondaryIndexType

/**
  * Global secondary index type
  */
case object DynamoGlobalSecondaryIndex extends DynamoSecondaryIndexType
