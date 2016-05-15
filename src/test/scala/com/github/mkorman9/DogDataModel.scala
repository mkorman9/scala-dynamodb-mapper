package com.github.mkorman9

case class DogDataModel(name: String,
                        furColors: Seq[String])

object DogsMapping extends DynamoTable[DogDataModel]("Dog") {
  val name = DynamoString("name")
  val furColors = DynamoStringSeq("furColors")

  override val _keys = (name, DynamoEmptyAttribute)
  override val _nonKeyAttributes = List(
    furColors
  )
}
