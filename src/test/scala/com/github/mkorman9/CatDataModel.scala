package com.github.mkorman9

import java.time.LocalDateTime

case class CatDataModel(name: String,
                        roleName: String,
                        mousesConsumed: Option[Int],
                        birthDate: LocalDateTime,
                        weight: Float)

object CatsMapping extends DynamoTable[CatDataModel]("Cat") {
  val roleName = DynamoString("roleName")
  val name = DynamoString("name")
  val mousesConsumed = DynamoInt("mousesConsumed", required = false)
  val birthDate = DynamoLocalDateTime("birthDate")
  val weight = DynamoFloat("weight")

  override val _keys = (roleName, name)
}
