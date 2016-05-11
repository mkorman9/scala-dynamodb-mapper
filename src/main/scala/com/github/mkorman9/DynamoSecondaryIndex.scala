package com.github.mkorman9

/**
  * Represents secondary index of DynamoDB table
  *
  * @param indexType Type of index, local or global
  */
abstract class DynamoSecondaryIndex(val indexType: DynamoSecondaryIndexType) {
  /**
    * Name of index in database
    */
  val name: String

  /**
    * Name of the attribute chosen to be a hash key
    */
  val hashKey: String

  /**
    * Name of the attribute chosen to be a sort key (could be left empty)
    */
  val sortKey: String = ""
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