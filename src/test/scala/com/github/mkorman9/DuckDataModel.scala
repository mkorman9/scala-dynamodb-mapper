package com.github.mkorman9

case class DuckDataModel(roleName: String,
                         weight: Long,
                         height: Long,
                         color: String)

object DucksMapping extends DynamoTable[DuckDataModel] {
  override val name = "Duck"
  override val hashKey = DynamoString("roleName")
  override val sortKey = DynamoLong("weight")
  override val attr = List(
    DynamoLong("height"),
    DynamoString("color")
  )
}

object DucksByHeightIndex extends DynamoSecondaryIndex {
  override val name: String = "DucksByHeight"
  override val indexType: SecondaryIndexType = LocalSecondaryIndex
  override val hashKey: String = "roleName"
  override val sortKey: String = "height"
}
