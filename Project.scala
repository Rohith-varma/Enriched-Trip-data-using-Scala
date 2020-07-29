package ca.rohith.bigdata

import java.io.{BufferedWriter, FileWriter}
import scala.io.Source

case class Trip(route_id: Int, service_id: String, trip_id: String, trip_headsign: String,
                direction_id: Int, shape_id: Int, wheelchair_accessible: Int, note_fr: String, note_en: String)

case class Route(route_id: Int, agency_id: String, route_short_name: Int, route_long_name: String,
                 route_type: Int, route_url: String, route_color: String, route_text_color: Any)

case class Calender(service_id: String, monday: Int, tuesday: Int, wednesday: Int, thursday: Int,
                    friday: Int, saturday: Int, sunday: Int, start_date: String, end_date: String)

case class TripRoute(Service_id: String, route: Route, trip: Trip)

case class EnrichedTrip(tripRoute: TripRoute, calender: Calender)

case class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.route_id -> route).toMap
  def lookup(routeId: Int): Route = lookupTable.getOrElse(routeId, null)
}

case class CalendarLookup(calendars: List[Calender]) {
  private val lookupTable: Map[String, Calender] =
    calendars.map(calendar => calendar.service_id -> calendar).toMap
  def lookup(serviceId: String): Calender = lookupTable.getOrElse(serviceId, null)
}

object Project extends App {
  def apply_route(csvline: List[String]): List[Route] = {
    var res = List[Route]()
    var i = 0
    for (c <- csvline) {
      if (i != 0) {
        var a: List[String] = c.split(",").toList
        if (a.length == 8) {
          var b = Route(a(0).toInt, a(1), a(2).toInt, a(3), (a(4).toInt), a(5), a(6), a(7))
          res = b :: res
        } else {
          var b = Route(a(0).toInt, a(1), a(2).toInt, a(3), (a(4).toInt), a(5), a(6), "null")
          res = b :: res
        }
      }
      i = i + 1
    }
    res
  }

  def apply_trip(csvline: List[String]): List[Trip] = {
    var res = List[Trip]()
    var i = 0
    for (c <- csvline) {
      if (i != 0) {
        var a: List[String] = c.split(",").toList
        if (a.length == 9) {
          var b = Trip(a(0).toInt, a(1), a(2), a(3), (a(4).toInt), a(5).toInt, a(6).toInt, a(7), a(8))
          res = b :: res
        } else {
          var b = Trip(a(0).toInt, a(1), a(2), a(3), (a(4).toInt), a(5).toInt, a(6).toInt, "null", "null")
          res = b :: res
        }
      }
      i = i + 1
    }
    res
  }

  def apply_calendar(csvline: List[String]): List[Calender] = {
    var res = List[Calender]()
    var i = 0
    for (c <- csvline){
      if (i != 0) {
        var a: List[String] = c.split(",").toList
        if (a.length == 10) {
          var b = Calender(a(0), a(1).toInt, a(2).toInt, a(3).toInt, (a(4).toInt), a(5).toInt, a(6).toInt, a(7).toInt, a(8), a(9))
          res = b :: res
        } else {
        //skipping first line as it is index of the CSV
        }
      }
      i = i + 1
    }
    res
  }

  var fileSource = Source.fromFile("C:/Users/Admin/Documents/MCIT/2nd Year/Functional Programming for Big Data/gtfs_stm/routes.txt").getLines.toList
  var route = apply_route(fileSource)
  var routes = new RouteLookup(route)

  fileSource = Source.fromFile("C:/Users/Admin/Documents/MCIT/2nd Year/Functional Programming for Big Data/gtfs_stm/trips.txt").getLines.toList
  var trips = apply_trip(fileSource)

  fileSource = Source.fromFile("C:/Users/Admin/Documents/MCIT/2nd Year/Functional Programming for Big Data/gtfs_stm/calendar.txt").getLines.toList
  val calendar = apply_calendar(fileSource)
  var calendars = CalendarLookup(calendar)


  var tripRouteData = trips.map(trip => {
    val route: Route = routes.lookup(trip.route_id)
    val Service_id: String = trip.service_id
    TripRoute(Service_id, route, trip)
  })

  var enrichTrip = tripRouteData.map(triproute => {
    val Service_id: String = triproute.Service_id
    val calender = calendars.lookup(Service_id)
    EnrichedTrip(triproute, calender)
  })



  val outputFile = new BufferedWriter(new FileWriter("C:/Users/Admin/Documents/MCIT/2nd Year/Functional Programming for Big Data/Project Output/EnrichedTrip.csv"))

  outputFile.write("service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date,route_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color\n")

  for (i <- enrichTrip) {
    for (j <- List(i.tripRoute)) {
      var serviceId = j.Service_id
      var k = calendars.lookup(serviceId)
      outputFile.write(k.service_id + "," + k.monday + "," + k.tuesday + "," + k.wednesday + "," + k.thursday + "," + k.friday + "," + k.saturday + "," + k.sunday + "," + k.start_date + "," + k.end_date + ",")
      for (k <- List(j.trip)) {
        outputFile.write(k.route_id + "," +k.trip_id + ","+ k.trip_headsign + "," +k.direction_id + "," + k.shape_id + "," + k.wheelchair_accessible + ","+ k.note_fr + ","+ k.note_en + ",")
      }
      for (k <- List(j.route)) {
        outputFile.write(k.agency_id + ","  + k.route_short_name + "," + k.route_long_name + "," + k.route_type + ","+ k.route_url+","+ k.route_color + "," + k.route_text_color)
      }
    }

    outputFile.write("\n")
  }

  outputFile.close()
}




