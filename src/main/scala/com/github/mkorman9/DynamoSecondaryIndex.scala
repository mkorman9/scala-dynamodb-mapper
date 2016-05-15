package com.github.mkorman9

/**
  * Represents secondary index of DynamoDB table
  *
  * @param _indexType Type of index, local or global
  */
abstract class DynamoSecondaryIndex[T <: DynamoTable[_]](nameInDatabase: String,
                                    val _indexType: DynamoSecondaryIndexType,
                                    val _sourceTable: T) extends DynamoDatabaseEntity(nameInDatabase) {
  override val _nonKeyAttributes = List()
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
