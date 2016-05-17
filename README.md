![build status](https://travis-ci.org/mkorman9/scala-dynamodb-mapper.svg?branch=dev)

## scala-dynamodb-mapper
Simple framework for mapping between Scala case classes and Amazon DynamoDB items. Wrapper around [awscala](https://github.com/seratch/AWScala)

## What does it do?
* Provides method for putting case class objects straight into DynamoDB
* Provides method for retrieving sequence of case class objects from DynamoDB
* Provides built-it mapping for basic Scala types and Joda's DateTime
* Allows you to write mappers for your own classes
* Allows you to declare attributes as non-required and map them to Option[T]
* Allows you to query tables using both local and global secondary indexes
* Provides simple DSL for queries

## What it doesn't do?
* Doesn't allow you to project query and get only selected attributes. It's only meant to be a mapper between database and a case class
* Doesn't provide a query cache

## How to install it?

Add a dependency in your own project with Maven

```
<dependency>
    <groupId>com.github.mkorman9</groupId>
    <artifactId>scala-dynamodb-mapper</artifactId>
    <version>0.4.50</version>
</dependency>
```

or SBT

```
libraryDependencies += "com.github.mkorman9" % "scala-dynamodb-mapper" % "0.4.50"
```

## How to use it?

At first you must establish connection with Amazon DynamoDB service:

```scala
import com.github.mkorman9._
import com.github.mkorman9.DynamoDSL._
import awscala.dynamodbv2._

implicit val dynamoDB = DynamoDB(YOUR_AWS_KEY, YOUR_AWS_KEY_ID)(Region.getRegion(Regions.EU_CENTRAL_1))
```

Then you create a data model, for example a Cat:

```scala
case class Cat(name: String,                 // Name of a cat
               roleName: String,             // It's role in the group
               mousesConsumed: Option[Int],  // Optional info about total number of mouses consumed
               birthDate: DateTime,          // Birth date (using Joda's DateTime) 
               furColors: Seq[String])       // Sequence of string describing colors of cat's fur
```

Lets assume that the roleName attribute is the hash key and the name is the sort key. Create table 'Cat' with these keys in AWS control panel.   

Create mapping for data model:

```scala
object Cats extends DynamoTable[Cat]("Cat") {
  val roleName = DynamoString("roleName")
  val name = DynamoString("name")
  val mousesConsumed = DynamoInt("mousesConsumed", required = false)
  val birthDate = DynamoDateTime("birthDate")
  val furColors = DynamoStringSeq("furColors")
  
  // You have to define tuple of your key attributes in format (hashKey, sortKey)
  // You can simply omit sortKey by putting DynamoEmptyAttribute in it's place if your table doesn't contain one
  override val _keys = (roleName, name)
}
```

Now you can add new cats to database

```scala
Cats.put(Cat("Matt", "Hunter", Some(57), new DateTime().minusYears(4), List("black", "white")))
Cats.put(Cat("Patt", "Hunter", Some(121), new DateTime().minusYears(7), List("brown", "white")))
```

And retrieve all the cats with role 'Hunter'

```scala
val hunters: Seq[Cat] = Cats.query(Cats.roleName === "Hunter")
```

You can also query the data using secondary index defined in database

```scala
object CatsByMousesConsumed extends DynamoSecondaryIndex("ByMousesConsumed", DynamoLocalSecondaryIndex, Cats) {
  override val _keys = (_sourceTable.roleName, _sourceTable.mousesConsumed)
}

val huntersWithOver4MousesConsumed: Seq[Cat] = Cats.query(CatsByMousesConsumed,
  Cats.roleName === "Hunter" and Cats.mousesConsumed > 4
)
```
