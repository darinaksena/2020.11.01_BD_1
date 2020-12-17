import java.time.{LocalDate, LocalDateTime, Period}
import java.time.format.DateTimeFormatter

import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions
import scala.collection.immutable.List
import java.io.{File, FileOutputStream, PrintWriter}
import java.time.temporal.ChronoUnit

import org.apache.commons.math3.util.FastMath.{pow, sqrt}

import runtime.ScalaRunTime.replStringOf
import runtime.ScalaRunTime.replStringOf

object Main {
  def main(args: Array[String]) {
    //Task 1
    //val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    //val sc = new SparkContext(cfg)
    //val textFile = sc.textFile("file:///C://Users//Daria_Aksenova//Desktop//21.txt")
    //val textFile = sc.textFile("file:///C://Users//Daria//Desktop//21.txt")
    //C:\Users\Daria\Desktop\Новая папка
        //textFile.foreach(println)
//    println("Hello world")
    //sc.stop()


    //Task 2
//    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
//    val sc = new SparkContext(cfg)
//    //val textFile = sc.textFile("file:///C://Users//Daria_Aksenova//Desktop//21.txt")
//    val textFile = sc.textFile("file:///C://Users//Daria//Desktop//21.txt")
//    //C:\Users\Daria\Desktop\Новая папка
//    textFile.foreach(println)
//    sc.stop()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)


    //Task 3
    // Stations analysis 1
//    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
//    val sc = new SparkContext(cfg)
//
//    val tripData = sc.textFile("file:///C://Users//Daria//Desktop//big_data//data//trips.csv")
//    // запомним заголовок, чтобы затем его исключить
//    val tripsHeader = tripData.first
//    val trips = tripData.filter(row=>row!=tripsHeader)
//      .map(row=>row.split(",",-1))
//    val stationData = sc.textFile("file:///C://Users//Daria//Desktop//big_data//data//stations.csv")
//    val stationsHeader = stationData.first
//    val stations = stationData.filter(row=>row!=stationsHeader)
//      .map(row=>row.split(",",-1))
//    println(tripsHeader)
//    println(stationsHeader)

//    sc.stop()

    //Task 4
    // Stations analysis 1
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)
//
    val tripData = sc.textFile("file:///C://Users//Daria//Desktop//big_data//data//trip.csv")
    val tripsHeader = tripData.first
    val trips = tripData.filter(row=>row!=tripsHeader)
      .map(row=>row.split(",",-1))
    val stationData = sc.textFile("file:///C://Users//Daria//Desktop//big_data//data//station.csv")
    val stationsHeader = stationData.first
    val stations = stationData.filter(row=>row!=stationsHeader)
      .map(row=>row.split(",",-1))
    println("Trips Header")
    println(tripsHeader)
    println("Stations Header")
    println(stationsHeader)
//
//
    val stationsIndexed = stations.keyBy(row=>row(0).toInt)
    stationsIndexed.collect()
    val tripsByStartTerminals = trips.keyBy(row=>row(4).toInt)
    val tripsByEndTerminals = trips.keyBy(row=>row(7).toInt)
    val startTrips =
      stationsIndexed.join(tripsByStartTerminals)

    val endTrips =
      stationsIndexed.join(tripsByEndTerminals)

    stations.take(10).foreach(println)
    println("stations")
    println("Stations Indexed")
    stationsIndexed.take(10).foreach(println);
//
//    println(startTrips.toDebugString)
//    println(endTrips.toDebugString)
//

//    println(startTrips.count())
//    println(endTrips.count())

//
//    stationsIndexed.partitionBy(new
//        HashPartitioner(trips.partitions.size))
//
//    println(stationsIndexed.partitioner)






    case class Station(
                        stationId:Integer,
                        name:String,
                        lat:Double,
                        long:Double,
                        dockcount:Integer,
                        landmark:String,
                        installation:String,
                        notes:String)
    case class Trip(
                     tripId:Integer,
                     duration:Integer,
                     startDate:LocalDateTime,
                     startStation:String,
                     startTerminal:Integer,
                     endDate:LocalDateTime,
                     endStation:String,
                     endTerminal:Integer,
                     bikeId: Integer,
                     subscriptionType: String,
                     zipCode: String)



    val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")


    val tripsInternal = trips.mapPartitions(rows => {
      val timeFormat =
      DateTimeFormatter.ofPattern("M/d/yyyy H:m")
      rows.map( row =>
        new Trip(tripId=row(0).toInt,
          duration=row(1).toInt,
          startDate= LocalDateTime.parse(row(2), timeFormat),
          startStation=row(3),
          startTerminal=row(4).toInt,
          endDate=LocalDateTime.parse(row(5), timeFormat),
          endStation=row(6),
          endTerminal=row(7).toInt,
          bikeId=row(8).toInt,
          subscriptionType=row(9),
          zipCode=row(10)))
    })

//    val sdHeader = stationData.count()
//    println(sdHeader)
    println("trips internal:")
    println(tripsInternal.first)
    println(tripsInternal.first.startDate)


    val stationsInternal = stations.map(row=>
      new Station(stationId=row(0).toInt,
        name=row(1),
        lat=row(2).toDouble,
        long=row(3).toDouble,
        dockcount=row(4).toInt,
        landmark=row(5),
        installation=row(6),
          notes=null))

    val tripsByStartStation = tripsInternal.keyBy(record =>
      record.startStation)

    val bikeInfo = tripsInternal.keyBy(record =>
      record.bikeId)

//    val stationsInternalCoord = stationsInternal.keyBy(record =>
//      (record.lat, record.long))
//
//    println("Coords by station")
//
//    stationsInternalCoord.take(10).foreach(println)


    println("Trips by station")

    tripsByStartStation.take(10).foreach(println)

    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x=>x.duration)
      .groupByKey()
      .mapValues(col=>col.reduce((a,b)=>a+b)/col.size)

    println("Avg duration by station")

    avgDurationByStartStation.take(10).foreach(println)

    val avgDurationByStartStation2 = tripsByStartStation
      .mapValues(x=>x.duration)
      .aggregateByKey((0,0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
      .mapValues(acc=>acc._1/acc._2)

    val firstGrouped = tripsByStartStation
      .groupByKey()
      .mapValues(x =>
        x.toList.sortWith((trip1, trip2) =>
          trip1.startDate.compareTo(trip2.startDate)<0))

    val firstGrouped2 = tripsByStartStation
      .reduceByKey((trip1,trip2) =>
        if (trip1.startDate.compareTo(trip2.startDate)<0)
          trip1 else trip2)

    //    1. Найти велосипед с максимальным пробегом.
    val bikesMileage = bikeInfo.mapValues(x=>x.duration)
      .groupByKey().mapValues(col=>col.reduce((a,b)=>a+b)).max()
    println("bike with max mileage:")
    println(bikesMileage._1)

    //    2. Найти наибольшее расстояние между станциями.

    val dataOfStations = stationsInternal.cartesian(stationsInternal)
      .map {
        case (station1, station2) =>
          (station1.long, station1.lat, station1.stationId,
            station2.long, station2.lat, station2.stationId)
      }


    def haversineDistance(a1: Double, a2: Double, b1: Double, b2: Double): Double = {
      val deltaLat = math.toRadians(b1 - a1)
      val deltaLong = math.toRadians(b2 - a2)
      val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(a1)) * math.cos(math.toRadians(b1)) * math.pow(math.sin(deltaLong / 2), 2)
      val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
      3958.761 * greatCircleDistance
    }


      val maxStationDistance = dataOfStations.map{ row => (sqrt(pow(row._1 - row._4,2) + pow(row._2 - row._5,2)), row._3 ,  row._6 ) }
      .sortBy( a => a._1,ascending = false)

    val maxStationDistance2 = dataOfStations.map{ row => (sqrt(pow(row._1 - row._4,2) + pow(row._2 - row._5,2)),
      row._1, row._2, row._3 , row._4, row._5,  row._6 ) }
      .sortBy( a => a._1,ascending = false)

    val pts = maxStationDistance2.first()
    println("Max distance (km):")
    println(haversineDistance(pts._2, pts._3, pts._5, pts._6))

    //    3. Найти путь велосипеда с максимальным пробегом через станции.
//    println("max mileage:")
//    println(bikesMileage._2)
    println("Way of bike: ")
    bikeInfo.lookup(bikesMileage._1).foreach(println)

//    val  wayOfMaxTrip = tripsInternal.keyBy(record => record.bikeId).lookup(bikesMileage._1).map(x=>x.startStation)
//    wayOfMaxTrip.foreach(println)

    //    4. Найти количество велосипедов в системе.
    val bikesCount = bikeInfo.mapValues(x=>x.bikeId)
      .groupByKey().distinct().count()
    println("Count distinct bikes:")
    println(bikesCount)

    //    5. Найти (велосипеды) потративших на поездки более 3 часов.
//    val bikesTime = bikeInfo.mapValues(x=>x.startDate)
//      .aggregateByKey((0,0))(
//        (acc, value) => (acc._1 + value, acc._2 + 1),
//        (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
//      .mapValues(acc=>acc._1/acc._2)
val bikesTime= tripsInternal.keyBy(record => record.bikeId).mapValues((x)=> x.duration ).filter(v => v._2 > 180)
    val bikesTime2 = tripsInternal.keyBy(record => record.bikeId).mapValues( row => ChronoUnit.MINUTES.between(row.startDate, row.endDate) )
    val bikesTime3 = bikesTime2.keyBy(record => record._1).mapValues((x)=> x._2 ).filter(v => v._2 > 180 )
//      .filter(v => v > 180)
    println("First 10 bikes with more than 3 hours of ride and their time: ")
    bikesTime3.take(10).foreach(println)

    sc.stop()
  }
}
