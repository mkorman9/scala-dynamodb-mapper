package com.github.mkorman9

import com.amazonaws.services.dynamodbv2.model.Condition

/**
  * Object providing additional options to queries DSL
  * Contains implicit classes that might be imported using import DynamoDSL._
  */
object DynamoDSL {
  type QueryParts = Seq[(String, Condition)]

  /**
    * Class that provides operators for joining parts of queries
    *
    * @param queryParts Base of query
    */
  implicit class QueryPartsJoiner(val queryParts: QueryParts) {
    /**
      * Adds second query part to base
      *
      * @param another Another query part to join
      * @return Joined queries
      */
    def and(another: QueryParts) = {
      queryParts ++ another
    }
  }
}
