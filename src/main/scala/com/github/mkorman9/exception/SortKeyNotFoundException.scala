package com.github.mkorman9.exception

/**
  * Thrown when sort key is not retrieved from database after query
  *
  * @param message Human readable message
  */
class SortKeyNotFoundException(message: String) extends AttributeNotFoundException(message)
