package com.github.mkorman9

import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.Condition

/**
  * Marker used for classes defining operators for queries DSL
  */
trait DynamoOperators {
  /**
    * Name of the database entity
    */
  val name: String
}

/**
  * Trait holding general operators used in queries DSL
  */
trait DynamoGeneralOperators extends DynamoOperators {
  /**
    * Equals operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def ===(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.eq(value))
  }

  /**
    * Not equals operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def !==(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.ne(value))
  }

  /**
    * Greater than operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def >(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.gt(value))
  }

  /**
    * Less than operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def <(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.lt(value))
  }

  /**
    * Greater or equal to operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def >=(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.ge(value))
  }

  /**
    * Less or equal to operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def <=(value: Any): Seq[(String, Condition)] = {
    Seq(this.name -> cond.le(value))
  }
}
