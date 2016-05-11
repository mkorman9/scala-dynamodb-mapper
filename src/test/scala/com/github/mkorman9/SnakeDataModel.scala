package com.github.mkorman9

case class SnakeDataModel(id: Long,
                          name: String,
                          color: String,
                          length: Long)

object SnakesMapping extends DynamoTable[SnakeDataModel] {
  override val name = "Snake"
  override val hashKey = DynamoLong("id")
  override val sortKey = DynamoString("name")
  override val attr = List(
    DynamoString("color"),
    DynamoLong("length")
  )

  object ByColor extends DynamoSecondaryIndex(DynamoGlobalSecondaryIndex) {
    override val name: String = "ByColor"
    override val hashKey: String = "color"
    override val sortKey: String = "name"
  }
}
