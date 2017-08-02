package com.github.mkorman9

case class DogDataModel(name: String,
                        furColors: Seq[String],
                        salary: BigDecimal)

object DogsMapping extends DynamoTable[DogDataModel]("Dog") {
  val name = DynamoString("name")
  val furColors = DynamoStringSeq("furColors")
  val salary = DynamoBigDecimal("salary")

  override val _keys = (name, DynamoEmptyAttribute)
}
