package com.github.mkorman9.exception

/**
  * Thrown when required attribute is not retrieved from database after query
  *
  * @param message Human readable message
  */
class AttributeNotFoundException(message: String) extends RuntimeException(message)
