package com.stratio.utils

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatterBuilder

import scala.util.{Success, Failure, Try}

object ParserUtils {

  val NOT_PARSEABLE_INT_ERROR: String = "Int Not Parseable"
  val NOT_PARSEABLE_DATE_ERROR: String = "Date Not Parseable"

  val dateTimeFormat = new DateTimeFormatterBuilder()
    .appendYear(2, 4)
    .appendLiteral('-')
    .appendMonthOfYear(1)
    .appendLiteral('-')
    .appendDayOfMonth(1)
    .toFormatter()

  def getDateTime(year: Int, month: Int, day: Int): DateTime =
    new DateTime(year, month, day, 0, 0, 0)

  def parseIntError(intToParse: String): Option[String] = intToParse.filter(!_.isDigit).isEmpty match{
    case true => None
    case _ => Some(NOT_PARSEABLE_INT_ERROR)
  }

  //TODO: Why another parsing method? The getDateTime could be wrapped within a Try and then we'd just have one
  def parseDate(stringDate: String): Option[String] = {
    Try { DateTime.parse(stringDate, dateTimeFormat)} match {
      case Failure(_) => Some(NOT_PARSEABLE_DATE_ERROR)
      case _ => None
    }
  }

  def parsePrice(yearMonthPrice: String): Option[FuelPrice] = {
    tryToParsePrice(yearMonthPrice) match {
      case Success(price) => Some(price)
      case Failure(_) => None
    }
  }

  def tryToParsePrice(yearMonthPrice: String): Try[FuelPrice] = {
    val Array(year, month, price) = yearMonthPrice.split(',')
    for {
      yearAsInt <- Try(year.toInt)
      monthAsInt <- Try(month.toInt)
      priceAsFloat <- Try(price.toFloat)
    } yield (FuelPrice(yearAsInt, monthAsInt, priceAsFloat))
  }

  def yearMonthFuelPrice(year: Int, month:Int, fuelPrice: RDD[FuelPrice]) =
    fuelPrice.filter(f => f.year == year && f.month == month).first().price
}

case class FuelPrice(year: Int, month: Int, price: Float)
