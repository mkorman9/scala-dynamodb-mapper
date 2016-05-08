package com.github.mkorman9.exception

/**
  * Thrown when hash key is not retrieved from database after query
  *
  * @param message Human readable message
  */
class HashKeyNotFoundException(message: String) extends AttributeNotFoundException(message)
