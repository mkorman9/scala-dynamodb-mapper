package com.github.mkorman9

case class DuckDataModel(color: String,
                         name: String,
                         height: Int)

object DucksMapping extends DynamoTable[DuckDataModel] {
  override val name = "Duck"
  override val hashKey = DynamoString("color")
  override val sortKey = DynamoInt("name")
  override val attr = List(
    DynamoInt("height")
  )

  object ByHeight extends DynamoSecondaryIndex {
    override val name: String = "ByHeight"
    override val indexType: DynamoSecondaryIndexType = DynamoLocalSecondaryIndex
    override val hashKey: String = "color"
    override val sortKey: String = "height"
  }
}
