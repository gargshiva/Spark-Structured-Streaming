package com.gargshiva

import java.sql.Timestamp

case class Stock(id: String, price: Double, timeStamp: java.sql.Timestamp)

object Stock {
  def apply(str: String): Stock = {
    val cols = str.split(",")
    Stock(cols(0), cols(1).toDouble, new Timestamp(cols(2).toLong))
  }
}