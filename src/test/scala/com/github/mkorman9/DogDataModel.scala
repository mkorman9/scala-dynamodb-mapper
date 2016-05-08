package com.github.mkorman9

case class DogDataModel(name: String,
                        furColors: Seq[String])

object DogsMapping extends DynamoTable[DogDataModel] {
  override val name = "Dog"
  override val hashKey = DynamoString("name")
  override val attr = List(
    DynamoStringSeq("furColors")
  )
}
