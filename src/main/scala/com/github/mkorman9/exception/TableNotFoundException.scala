package com.github.mkorman9.exception

/**
  * Thrown when table is not found in the database
  *
  * @param message Human readable message
  */
class TableNotFoundException(message: String) extends RuntimeException(message)
