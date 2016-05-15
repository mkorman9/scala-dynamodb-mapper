package com.github.mkorman9

import org.joda.time.DateTime

case class CatDataModel(name: String,
                        roleName: String,
                        mousesConsumed: Option[Int],
                        birthDate: DateTime)

object CatsMapping extends DynamoTable[CatDataModel]("Cat") {
  val roleName = DynamoString("roleName")
  val name = DynamoString("name")
  val mousesConsumed = DynamoInt("mousesConsumed", required = false)
  val birthDate = DynamoDateTime("birthDate")

  override val _keys = (roleName, name)
  override val _nonKeyAttributes = List(
    mousesConsumed,
    birthDate
  )
}
