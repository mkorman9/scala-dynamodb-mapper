package com.github.mkorman9.exception

/**
  * Thrown when secondary index is not found in the database
  *
  * @param message Human readable message
  */
class SecondaryIndexNotFoundException(message: String) extends RuntimeException(message)
