package com.github.mkorman9

import java.time.LocalDateTime

import awscala.dynamodbv2.{DynamoDB, _}
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scalatest._
import com.github.mkorman9.DynamoDSL._

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
      ("color", ScalarAttributeType.S),
      ("name", ScalarAttributeType.S),
      Seq(
        ("height", ScalarAttributeType.N)
      ),
      Seq(
        LocalSecondaryIndex(
          name = "ByHeight",
          keySchema = Seq(KeySchema("color", KeyType.Hash), KeySchema("height", KeyType.Range)),
          projection = Projection(ProjectionType.All)
        )
      )
    )

    val snakesTable = Table(
      name = "Snake",
      hashPK = "id",
      rangePK = Some("name"),
      attributes = Seq(
        AttributeDefinition("id", ScalarAttributeType.N),
        AttributeDefinition("name", ScalarAttributeType.S),
        AttributeDefinition("color", ScalarAttributeType.S)
      ),
      localSecondaryIndexes = Seq(),
      globalSecondaryIndexes = Seq(
        GlobalSecondaryIndex(
          name = "ByColor",
          keySchema = Seq(KeySchema("color", KeyType.Hash), KeySchema("name", KeyType.Range)),
          projection = Projection(ProjectionType.All),
          provisionedThroughput = ProvisionedThroughput(5, 5)
        )
      ),
      provisionedThroughput = None
    )
    connection.createTable(snakesTable)
  }

  test("Mapper should persist correct set of data with hash and sort key") {
    val catToDelete = CatDataModel("Leila", "Hunter", None, LocalDateTime.now().minusYears(12), 10f)
    val catsWithMousesOver100 = List(
      CatDataModel("Johnny", "Hunter", Some(112), LocalDateTime.now().minusYears(1), 12.5f),
      CatDataModel("Pablo", "Hunter", Some(117), LocalDateTime.now().minusYears(1), 14.1f)
    )
    val cats = List(
      CatDataModel("Mike", "Worker", Some(41), LocalDateTime.now().minusYears(3), 14.2f),
      CatDataModel("Ricky", "Unemployed", None,LocalDateTime.now().minusYears(2), 17f),
      catToDelete
    ) ::: catsWithMousesOver100

    CatsMapping.putAll(cats)

    val huntersBeforeRemoving = CatsMapping query {
      CatsMapping.roleName === "Hunter"
    }
    val catToDeleteFromDb = CatsMapping.get("Hunter", "Leila")

    CatsMapping.delete("Hunter", "Leila")

    val huntersAfterRemoving = CatsMapping query {
      CatsMapping.roleName === "Hunter"
    }
    val deletedCat = CatsMapping.get("Hunter", "Leila")

    val catsWithMousesOver100FromDb = CatsMapping scan { CatsMapping.mousesConsumed > 100 }

    catToDeleteFromDb should be (Some(catToDelete))
    deletedCat should be (None)
    huntersBeforeRemoving.size should be(3)
    huntersAfterRemoving.size should be(2)
    huntersBeforeRemoving forall (cats.contains(_)) should be(true)
    huntersAfterRemoving forall (cats.contains(_)) should be(true)
    catsWithMousesOver100FromDb.toSet should be (catsWithMousesOver100.toSet)
  }

  test("Mapper should persist correct set of data with just hash key") {
    val dogToDelete = DogDataModel("Max", List("black", "white"), BigDecimal("1212123.3453453"))
    val dogs = List(
      DogDataModel("Rex", List("brown", "white"), BigDecimal("1212123.3453453")),
      dogToDelete
    )

    DogsMapping.putAll(dogs)

    val maxBeforeRemoving = DogsMapping query {
      DogsMapping.name === "Max"
    }
    val dogToDeleteFromDb = DogsMapping.get("Max")

    DogsMapping.delete("Max")

    val maxAfterRemoving = DogsMapping query {
      DogsMapping.name === "Max"
    }
    val deletedDog = DogsMapping.get("Max")

    dogToDeleteFromDb should be (Some(dogToDelete))
    deletedDog should be (None)
    maxBeforeRemoving.size should be(1)
    maxAfterRemoving.size should be(0)
    dogs.contains(maxBeforeRemoving.head) should be(true)
  }

  test("Mapper should retrieve set of data using local secondary index") {
    val whiteDucksOver4 = List(
      DuckDataModel("White", "John", 5),
      DuckDataModel("White", "Paul", 6)
    )
    val ducks = List(
      DuckDataModel("Black", "Raul", 5),
      DuckDataModel("White", "Mike", 3)
    ) ::: whiteDucksOver4

    DucksMapping.putAll(ducks)

    val whiteDucksOver4FromDb = DucksMapping query(DucksMapping.ByHeight,
      DucksMapping.color === "White" and DucksMapping.height > 4
    )

    whiteDucksOver4FromDb.toSet should be (whiteDucksOver4.toSet)
  }

  test("Mapper should retrieve set of data using global secondary index") {
    val whiteAkensSnakes = List(
      SnakeDataModel(0, "Akens", "White", 5),
      SnakeDataModel(1, "Akens", "White", 6)
    )
    val snakes = List(
      SnakeDataModel(2, "Black", "Paul", 3),
      SnakeDataModel(3, "Green", "Ryan", 6)
    ) ::: whiteAkensSnakes

    SnakesMapping.putAll(snakes)

    val whiteAkensSnakesFromDb = SnakesMapping.query(SnakesMapping.ByColor,
      SnakesMapping.color === "White" and SnakesMapping.name === "Akens"
    )

    whiteAkensSnakesFromDb.toSet should be (whiteAkensSnakes.toSet)
  }
}
