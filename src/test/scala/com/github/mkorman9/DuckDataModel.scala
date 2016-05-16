package com.github.mkorman9

case class DuckDataModel(color: String,
                         name: String,
                         height: Int)

object DucksMapping extends DynamoTable[DuckDataModel]("Duck") {
  val color = DynamoString("color")
  val name = DynamoString("name")
  val height = DynamoInt("height")

  override val _keys = (color, name)

  object ByHeight extends DynamoSecondaryIndex("ByHeight", DynamoLocalSecondaryIndex, DucksMapping) {
    override val _keys = (_sourceTable.color, _sourceTable.height)
  }
}
