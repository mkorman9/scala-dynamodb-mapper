package com.github.mkorman9

case class SnakeDataModel(id: Long,
                          name: String,
                          color: String,
                          length: Long)

object SnakesMapping extends DynamoTable[SnakeDataModel]("Snake") {
  val id = DynamoLong("id")
  val name = DynamoString("name")
  val color = DynamoString("color")
  val length = DynamoLong("length")

  override val _keys = (id, name)

  object ByColor extends DynamoSecondaryIndex("ByColor", DynamoGlobalSecondaryIndex, SnakesMapping) {
    override val _keys = (_sourceTable.color, _sourceTable.name)
  }
}
