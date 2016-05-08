package com.github.mkorman9

/**
  * Represents secondary index mapping
  */
abstract class DynamoSecondaryIndex {
  /**
    * Name of index in database
    */
  val name: String

  /**
    * Type of index (local or global)
    */
  val indexType: SecondaryIndexType

  /**
    * Attribute chosen to be a hash key
    */
  val hashKey: String

  /**
    * Attribute chosen to be a sort key. Sort key is not required in table
    */
  val sortKey: String = ""
}

/**
  * Type of index (local or global)
  */
sealed trait SecondaryIndexType

/**
  * Local index type
  */
case object LocalSecondaryIndex extends SecondaryIndexType

/**
  * Global index type
  */
case object GlobalSecondaryIndex extends SecondaryIndexType
