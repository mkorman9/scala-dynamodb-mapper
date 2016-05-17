package com.github.mkorman9

import com.amazonaws.services.dynamodbv2.model.Condition

object DynamoDSL {
  implicit class QueryPartsJoiner(val querySequence: Seq[(String, Condition)]) {
    def and(other: Seq[(String, Condition)]) = {
      querySequence ++ other
    }
  }
}
