package com.github.mkorman9

import com.amazonaws.services.dynamodbv2.model.Condition

/**
  * Object providing additional options to queries DSL
  * Contains implicit classes that might be imported using import DynamoDSL._
  */
object DynamoDSL {

  /**
    * Class that provides operators for joining parts of queries
    *
    * @param querySequence Base of query
    */
  implicit class QueryPartsJoiner(val querySequence: Seq[(String, Condition)]) {
    /**
      * Adds second query part to base
      *
      * @param another Another query part to join
      * @return Joined queries
      */
    def and(another: Seq[(String, Condition)]) = {
      querySequence ++ another
    }
  }
}
