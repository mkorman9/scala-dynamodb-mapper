package com.github.mkorman9

import awscala.dynamodbv2.DynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import awscala.dynamodbv2._

class DynamoIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var connection: DynamoDB = _

  override def beforeAll = {
    connection = DynamoDB.local()
    connection.createTable("Cat",
      ("roleName", ScalarAttributeType.S),
      ("name", ScalarAttributeType.S),
      Seq(
        ("mousesConsumed", ScalarAttributeType.N),
        ("birthDate", ScalarAttributeType.N)
      ),
      Seq()
    )
  }

  "Mapper" should "persist correct set of data" in {
    val cats = List(CatDataModel("Johnny", "Hunter", Some(112), new DateTime().minusYears(7)),
      CatDataModel("Mike", "Worker", Some(41), new DateTime().minusYears(3)),
      CatDataModel("Pablo", "Hunter", Some(117), new DateTime().minusYears(1)),
      CatDataModel("Ricky", "Unemployed", None, new DateTime().minusYears(2)),
      CatDataModel("Leila", "Hunter", None, new DateTime().minusYears(12))
    )

    CatsMapping.put(cats(0))
    CatsMapping.put(cats(1))
    CatsMapping.put(cats(2))
    CatsMapping.put(cats(3))
    CatsMapping.put(cats(4))

    val huntersBeforeRemoving = CatsMapping query { "roleName" -> cond.eq("Hunter") :: Nil }
    CatsMapping.delete("Hunter", "Leila")
    val huntersAfterRemoving = CatsMapping query { "roleName" -> cond.eq("Hunter") :: Nil }

    huntersBeforeRemoving.size should be (3)
    huntersAfterRemoving.size should be (2)
    huntersBeforeRemoving forall (cats.contains(_)) should be (true)
    huntersAfterRemoving forall (cats.contains(_)) should be (true)
  }
}
