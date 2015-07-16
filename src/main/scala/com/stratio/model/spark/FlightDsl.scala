package com.stratio.model.spark

import com.stratio.utils.ParserUtils
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.Map
import scala.language.implicitConversions
import com.stratio.model.Flight
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


class FlightCsvReader(self: RDD[String]) {

  /**
   *
   * Parser the csv file with the format described in the readme.md file to a Flight class
   *
   */
  def toFlight: RDD[Flight] = self.map { flightAsString => Flight(flightAsString.split(','))}

  /**
   *
   * Obtain the parser errors
   *
   */
  def toErrors: RDD[(String, String)] = for {
    flightAsString <- self;
    error <- Flight.extractErrors(flightAsString.split(','))
    if (!error.isEmpty)
  } yield (flightAsString, error)
}
  class FlightFunctions(self: RDD[Flight]) {

    def toBroadcastMapPriceByYearAndMonth(prices: RDD[String]): Broadcast[Map[(Int, Int), Float]] =
      self.sparkContext.broadcast(prices.flatMap(priceAsString =>
        ParserUtils.parsePrice(priceAsString)).map(price =>
        ((price.year, price.month), price.price)).collect.toMap)

    /**
     *
     * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Year, Month
     *
     */
    def minFuelConsumptionByMonthAndAirport(fuelPrices: RDD[String]): RDD[(String, (Short, Short))] = {
      val reduceFunction = (v1: (Int, Int, Float), v2: (Int, Int, Float)) => (if (v1._3 < v2._3) v1 else v2)
      val priceCatalogue = toBroadcastMapPriceByYearAndMonth(fuelPrices)
      self.map(f =>
        ((f.origin, f.date.getYear, f.date.getMonthOfYear),
          f.distance * priceCatalogue.value((f.date.getYear, f.date.getMonthOfYear)))).reduceByKey(_ + _)
        .map(e => ((e._1._1), (e._1._2, e._1._3, e._2)))
        .reduceByKey(reduceFunction)
        .map(e => (e._1, (e._2._1.toShort, e._2._2.toShort)))
    }

    def getFuelPriceByMonthAndYear(fuelPrices: RDD[(Int, Int, Float)], year: Int, month: Int) =
      fuelPrices.filter(fuelPrice => (fuelPrice._1 == year && fuelPrice._2 == month)).first._3

    /**
     *
     * Obtain the average distance fliyed by airport, taking the origin field as the airport to group
     *
     */
    def averageDistanceByAirport: RDD[(String, Float)] = {
      val reduceFunction = (v1: (Float, Float), v2: (Float, Float)) => ((v1._1 + v2._1), (v1._2 + v2._2))
      val valuesReduced = self.map(f =>
        (f.origin, f.distance.toFloat)).mapValues(v =>
        (v,1.0F)).reduceByKey(reduceFunction)
      valuesReduced.map{ case (key, value) => (key, value._1 / value._2.toFloat) }
    }

  /**
    * A Ghost Flight is each flight that has arrTime = -1 (that means that the flight didn't land where was supposed to
    * do it).
    *
    * The correct assign to those flights will be :
    *
    * The ghost's flight arrTime would take the data of the nearest flight in time that has its flightNumber and which
    * deptTime is lesser than the ghost's flight deptTime plus a configurable amount of seconds, from here we will call
    * this flight: the saneated Flight
    *
    * So if there is no sanitized Flight for a ghost Flight we will return the same ghost Flight but if there are some
    * sanitized Flight we had to assign the following values to the ghost flight:
    *
    * dest = sanitized Flight origin
    * arrTime = sanitized Flight depTime
    * csrArrTime = sanitized Flight csrDepTime
    * date =  sanitized Flight date
    *
    * --Example
    *   window time = 600 (10 minutes)
    *   flights before resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour=810 arrTime=-1 orig=A dest=D
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour=810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour=821 arrTime=855 orig=A dest=D
    *  flights after resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour=810 arrTime=815 orig=A dest=B
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour=810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour=821 arrTime=855 orig=A dest=D
    */
    def assignGhostFlights(elapsedSeconds: Int): RDD[Flight] = {
      val flightsByNumber = self.keyBy(flight => flight.flightNum).groupByKey()
      flightsByNumber.map(flightByNumber => {
        if (flightByNumber._2.find(flight => flight.arrTime == -1).isDefined) {
          val flightsWithOneGhost = flightByNumber._2
          val flightCandidates = flightsWithOneGhost.filter(f => f.arrTime != -1)
          val ghostFlight = flightsWithOneGhost.filter(f => f.arrTime == -1).head
          val sanitizedFlightCandidates = for {
            f <- flightCandidates
            if (f.departureTime < (ghostFlight.departureTime + elapsedSeconds))
          } yield f
          if (sanitizedFlightCandidates.isEmpty) flightByNumber
          else {
            val sanitizedCandidate = sanitizedFlightCandidates.minBy(f => f.departureTime)
            val sanitizedFlight = ghostFlight.copy(arrTime = sanitizedCandidate.departureTime)
            (flightByNumber._1, flightCandidates ++ Seq(sanitizedFlight))
          }
        } else {
          flightByNumber
        }
      }
      ).flatMap(flights => flights._2)
    }
  }


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl

object GhostFlightsFunctions {

}
