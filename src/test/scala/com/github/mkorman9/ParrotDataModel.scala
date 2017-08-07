package com.github.mkorman9

case class ParrotInfo(name: String,
                      age: Int)

case class ParrotDataModel(id: Long,
                           info: ParrotInfo)

object ParrotsMapping extends DynamoTable[ParrotDataModel]("Parrot") {
  val id: DynamoLong = DynamoLong("id")
  val info: DynamoJSON[ParrotInfo] = DynamoJSON[ParrotInfo]("info")

  override val _keys = (id, DynamoEmptyAttribute)
}
