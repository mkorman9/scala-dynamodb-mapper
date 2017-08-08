![build status](https://travis-ci.org/mkorman9/scala-dynamodb-mapper.svg?branch=master)

## scala-dynamodb-mapper
Simple framework for mapping between Scala classes and Amazon DynamoDB tables. Wrapper around [awscala](https://github.com/seratch/AWScala)

## What does it do?
* Provides method for putting objects straight into DynamoDB
* Provides method for retrieving sequence of objects from DynamoDB
* Provides built-it mapping for basic Scala types and Java DateTime API
* Allows you to write mappers for your own classes
* Allows you to declare attributes as non-required and map them to Option[T]
* Allows you to query tables using both local and global secondary indexes
* Provides simple DSL for queries

## What it doesn't do?
* Doesn't allow you to project query and get only selected attributes. It's only meant to be a mapper between database and a class
* Doesn't provide a query cache

## How to install it? / How to use it?

All examples can be found on wiki, in [Getting Started](https://github.com/mkorman9/scala-dynamodb-mapper/wiki) section.

## How to contribute?

* Identify missing feature or bug to fix
* Create feature branch to work on. It is the best idea to choose *develop* as source branch, to avoid problems with merge.
* Apply your changes and push it
* Create pull request against *develop* branch, and wait for comments/approve
