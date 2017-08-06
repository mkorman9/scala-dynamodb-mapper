package com.github.mkorman9

import awscala.dynamodbv2._
import com.github.mkorman9.DynamoDSL.QueryParts

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
  def ===(value: Any): QueryParts = {
    Seq(this.name -> cond.eq(value))
  }

  /**
    * Not equals operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def !==(value: Any): QueryParts = {
    Seq(this.name -> cond.ne(value))
  }

  /**
    * Greater than operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def >(value: Any): QueryParts = {
    Seq(this.name -> cond.gt(value))
  }

  /**
    * Less than operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def <(value: Any): QueryParts = {
    Seq(this.name -> cond.lt(value))
  }

  /**
    * Greater or equal to operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def >=(value: Any): QueryParts = {
    Seq(this.name -> cond.ge(value))
  }

  /**
    * Less or equal to operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def <=(value: Any): QueryParts = {
    Seq(this.name -> cond.le(value))
  }

  /**
    * Not null operator used in queries DSL
    *
    * @return Element of query used in DSL
    */
  def isNotNull: QueryParts = {
    Seq(this.name -> cond.isNotNull)
  }

  /**
    * Is null operator used in queries DSL
    *
    * @return Element of query used in DSL
    */
  def isNull: QueryParts = {
    Seq(this.name -> cond.isNull)
  }

  /**
    * Between operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def between(value: Any*): QueryParts = {
    Seq(this.name -> cond.between(value))
  }

  /**
    * In operator used in queries DSL
    *
    * @param value Value to compare with
    * @return Element of query used in DSL
    */
  def in(value: Any*): QueryParts = {
    Seq(this.name -> cond.in(value))
  }
}

/**
  * Trait holding operators used in queries DSL specific to collection values
  */
trait DynamoCollectionOperators extends DynamoOperators {
  /**
    * Contains operator used in queries DSL
    *
    * @return Element of query used in DSL
    */
  def contains(value: Any*): QueryParts = {
    Seq(this.name -> cond.contains(value))
  }

  /**
    * Not contains operator used in queries DSL
    *
    * @return Element of query used in DSL
    */
  def notContains(value: Any*): QueryParts = {
    Seq(this.name -> cond.notContains(value))
  }
}

/**
  * Trait holding operators used in queries DSL specific to string values
  */
trait DynamoStringOperators extends DynamoOperators {
  /**
    * Begins with operator used in queries DSL
    *
    * @return Element of query used in DSL
    */
  def beginsWith(value: Any*): QueryParts = {
    Seq(this.name -> cond.beginsWith(value))
  }
}
