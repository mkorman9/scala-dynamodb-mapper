package com.github.mkorman9.exception

/**
  * Thrown when secondary index with specified name is not found in database
  *
  * @param message Human readable message
  */
class SecondaryIndexNotFoundException(message: String) extends AttributeNotFoundException(message)
