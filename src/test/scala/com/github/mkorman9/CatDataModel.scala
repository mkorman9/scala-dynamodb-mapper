package com.github.mkorman9

import org.joda.time.DateTime

case class CatDataModel(name: String,
                        roleName: String,
                        mousesConsumed: Option[Int],
                        birthDate: DateTime)

object CatsMapping extends DynamoTable[CatDataModel] {
  override val name = "Cat"
  override val hashKey = DynamoString("roleName")
  override val sortKey = DynamoString("name")
  override val attr = List(
      DynamoInt("mousesConsumed", required = false),
      DynamoDateTime("birthDate")
    )
}
