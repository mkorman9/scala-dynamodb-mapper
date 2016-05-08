package com.github.mkorman9

import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.joda.time.DateTime
import org.scalatest._

class DynamoIntegrationTest extends FunSuite with Matchers with BeforeAndAfterAll {
  implicit var connection: DynamoDB = _

  override def beforeAll = {
    connection = DynamoDB.local()
    connection.createTable("Cat",
      ("roleName", ScalarAttributeType.S),
      ("name", ScalarAttributeType.S),
      Seq(),
      Seq()
    )

    connection.createTable("Dog",
      ("name", ScalarAttributeType.S)
    )

    connection.createTable("Duck",
      ("roleName", ScalarAttributeType.S),
      ("weight", ScalarAttributeType.N),
      Seq(
        ("height", ScalarAttributeType.N),
        ("weight", ScalarAttributeType.N)
      ),
      Seq(
        awscala.dynamodbv2.LocalSecondaryIndex(
          "DucksByHeight",
          Seq(KeySchema("roleName", KeyType.Hash), KeySchema("height", KeyType.Range)),
          Projection(ProjectionType.All)
        )
      )
    )
  }

  test("Mapper should persist correct set of data with hash and sort key") {
    val cats = List(CatDataModel("Johnny", "Hunter", Some(112), new DateTime().minusYears(7)),
      CatDataModel("Mike", "Worker", Some(41), new DateTime().minusYears(3)),
      CatDataModel("Pablo", "Hunter", Some(117), new DateTime().minusYears(1)),
      CatDataModel("Ricky", "Unemployed", None, new DateTime().minusYears(2)),
      CatDataModel("Leila", "Hunter", None, new DateTime().minusYears(12))
    )

    CatsMapping.putAll(cats)

    val huntersBeforeRemoving = CatsMapping query {
      "roleName" -> cond.eq("Hunter") :: Nil
    }
    CatsMapping.delete("Hunter", "Leila")
    val huntersAfterRemoving = CatsMapping query {
      "roleName" -> cond.eq("Hunter") :: Nil
    }

    huntersBeforeRemoving.size should be(3)
    huntersAfterRemoving.size should be(2)
    huntersBeforeRemoving forall (cats.contains(_)) should be(true)
    huntersAfterRemoving forall (cats.contains(_)) should be(true)
  }

  test("Mapper should persist correct set of data with just hash key") {
    val dogs = List(DogDataModel("Max", List("black", "white")),
      DogDataModel("Rex", List("brown", "white"))
    )

    DogsMapping.putAll(dogs)

    val maxBeforeRemoving = DogsMapping query {
      "name" -> cond.eq("Max") :: Nil
    }
    DogsMapping.delete("Max")
    val maxAfterRemoving = DogsMapping query {
      "name" -> cond.eq("Max") :: Nil
    }

    maxBeforeRemoving.size should be(1)
    maxAfterRemoving.size should be(0)
    dogs.contains(maxBeforeRemoving.head) should be(true)
  }

  test("Mapper should find entity by secondary index") {
    val ducks = List(DuckDataModel("Spotter", 1, 10, "white"),
      DuckDataModel("Spotter", 2, 13, "white"),
      DuckDataModel("Spotter", 2, 12, "black"),
      DuckDataModel("Eater", 4, 15, "white")
    )

    DucksMapping.putAll(ducks)
    val spottersTallerThan11 = DucksMapping.query(DucksByHeightIndex, Seq("roleName" -> cond.eq("Spotter"), "height" -> cond.gt(11)))

    spottersTallerThan11.size should be(2)
    spottersTallerThan11 forall (ducks.contains(_)) should be(true)
  }
}
