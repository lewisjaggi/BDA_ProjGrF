import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.cloudera.science.geojson.{Feature, FeatureCollection}
import org.apache.spark.sql._
import org.apache.spark._

import scala.collection.immutable.Range
import scala.math.Numeric.Implicits.infixNumericOps

object taxi {
  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    import org.apache.spark.sql.functions._

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    val spark = SparkSession.builder
      .master("local[*]")
      .appName("My App")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    val taxiRaw = spark.read.format("csv").option("header", "true") .load("taxidata\\trip_data_1.csv")

    //taxiRaw = taxiRaw.map[RichRow](x => new RichRow(x))

    import spark.implicits._
    val safeParse = safe(parse)
    val taxiParsed = taxiRaw.rdd.map(safeParse)
    //taxiParsed.map(_.isLeft).countByValue().foreach(println)


    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }


    val taxiGood = taxiParsed.map(_.left.getOrElse(Trip("",0,0,0,0,0,0,0,0,0,0))).toDS().filter(t => t.license != "")
    taxiGood.cache()
    taxiGood.show(10)


    import org.apache.spark.sql.functions.udf
    val hoursUDF = udf(hours)
    /*taxiGood.
      groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
      count().
      sort("h").
      show()*/


    val textFromSource = scala.io.Source.fromFile("taxidata/nyc-boroughs.geojson")
    val geojson = textFromSource.mkString
    textFromSource.close()

    import com.cloudera.science.geojson
    import com.cloudera.science.geojson.GeoJsonProtocol._
    import spray.json._

    val features = geojson.parseJson.convertTo[FeatureCollection]

    import com.esri.core.geometry.Point
    val p = new Point(-73.994499, 40.75066)
    val borough = features.find(f => f.geometry.contains(p))

    import org.apache.spark.sql.functions._


    /*taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
      count().
      sort("h").
      show()

    taxiGood.where(hoursUDF($"pickupTime", $"dropoffTime") < 0).
      collect().
      foreach(println)*/

    spark.udf.register("hours", hours)
   /* val taxiClean = taxiGood.where(
      "hours(pickupTime, dropoffTime) BETWEEN 0 AND 3"
    )*/

    val areaSortedFeatures = features.sortBy(f => {
      val borough = f("cartodb_id").convertTo[Int]
      (borough, -f.geometry.area2D())
    })

    val bFeatures =  sc.broadcast(areaSortedFeatures)
    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("name").convertTo[String]
      }).getOrElse("NA")
    }
    val boroughUDF = udf(bLookup)
   /* taxiClean.
      groupBy(boroughUDF($"dropoffX", $"dropoffY")).
      count().
      show()
    taxiClean.
      where(boroughUDF($"dropoffX", $"dropoffY") === "NA").
      show()
    val taxiDone = taxiClean.where(
      "dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0"
    ).cache()
    taxiDone.
      groupBy(boroughUDF($"dropoffX", $"dropoffY")).
      count().
      show()*/



    //AVERAGE SPEED

    val speed = (tripDist: Double, tripTime: Long) => {
      toKm(tripDist) / (tripTime / 3600.0)
    }
    val speedUDF = udf(speed)
    spark.udf.register("speed", speed)

    val taxiSpeed = taxiGood.filter(t => t.tripDistance != 0 && t.tripTimeInSecs != 0)
      .where(boroughUDF($"dropoffX", $"dropoffY") === boroughUDF($"pickupX", $"pickupY") && boroughUDF($"dropoffX", $"dropoffY") =!= "NA")
      .withColumn("Borough", boroughUDF($"dropoffX", $"dropoffY"))
      .withColumn("AvgSpeed", speedUDF($"tripDistance", $"tripTimeInSecs"))
      .filter($"AvgSpeed" < 90)
    taxiSpeed.cache();

    taxiSpeed.groupBy($"Borough").agg(avg($"AvgSpeed").as("AvgSpeed")).orderBy(desc("AvgSpeed")).show(10)
   // println("Average :" + taxiSpeed.select(avg("AvgSpeed")).head());
    //println("Median : " +  taxiSpeed.stat.approxQuantile("AvgSpeed", Array(0.5), 0.001).head)

    //BEST HOUR IN EACH BOROUGH


    taxiGood.withColumn("Borough", boroughUDF($"pickupX", $"pickupY"))
      .select($"hourTime", $"Borough")
      .where($"Borough" =!= "NA")
      .groupBy($"hourTime",  $"Borough")
      .count()
      .orderBy($"Borough", desc("count"))
      .dropDuplicates("Borough")
      .show(200)

    //NUMBER OF PEOPLE TAKE BY ONE TAXY


    val day = (time: Long) => {
      TimeUnit.DAYS.convert(time, TimeUnit.MILLISECONDS)
    }
    val dayUDF = udf(day)
    spark.udf.register("day", day)

    println("NB License  : " +  taxiGood.select($"license").distinct().count())
    val taxiLPD = taxiGood.withColumn("day", dayUDF($"pickupTime"))
      .select($"license", $"people", $"day").where($"license" =!= "null")
      taxiLPD.cache()

    /*taxiLPD.groupBy($"license")
      .sum("people")
      .groupBy($"day")
      .avg("sum(people)")
      .orderBy(desc("avg(people)"))
      .show()*/

   /* val avgPerDay = (peopleNumber: Long) => {
      peopleNumber / 31.0;
    }
    val avgPerDayUDF = udf(avgPerDay)

      taxiLPD.groupBy($"license")
      .agg(sum("people"))
      .withColumn("AvgPerDay", avgPerDayUDF($"sum(people)"))
      .orderBy(desc("AvgPerDay"))
      .show()*/

    /*val listRes = List(List())

    taxiGood.select($"license").distinct().collect().foreach(row  =>{
      taxiLPD.where($"license" === row.get(0)).groupBy($"day").sum().select(avg("sum(people)")).collect() :: listRes
    }
    )*/

    val days  = taxiLPD.groupBy($"license").agg(countDistinct("day").as("dayDistinc")).orderBy($"license").collect() //select("day").distinct().count()
    val peoples =  taxiLPD.groupBy($"license").agg(sum($"people")).orderBy($"license").collect()

    (days, peoples).zipped.map{ case (d, p) => println(p.get(0).asInstanceOf[String] + " :  " + p.get(1).asInstanceOf[Long].toFloat / d.get(1).asInstanceOf[Long])}



    /*
        val sessions = taxiDone.
          repartition($"license").
          sortWithinPartitions($"license", $"pickupTime")
        sessions.cache()

        def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
          val b = bLookup(t1.dropoffX, t1.dropoffY)
          val d = (t2.pickupTime - t1.dropoffTime) / 1000
          (b, d)
        }



        val boroughDurations: DataFrame =
          sessions.mapPartitions(trips => {
            val iter: Iterator[Seq[Trip]] = trips.sliding(2)
            val viter = iter.
              filter(_.size == 2).
              filter(p => p(0).license == p(1).license)
            viter.map(p => boroughDuration(p(0), p(1)))
          }).toDF("borough", "seconds")
        boroughDurations.
          selectExpr("floor(seconds / 3600) as hours").
          groupBy("hours").
          count().
          sort("hours").
          show()
        boroughDurations.
          where("seconds > 0 AND seconds < 60*60*4").
          groupBy("borough").
          agg(avg("seconds"), stddev("seconds")).
          show()*/



  }


  def toKm(miles: Double): Double = {
    return miles * 1.60934
  }
  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }
  def parseTaxiTimeHour(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val parser = new SimpleDateFormat(
      "HH", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => parser.format(formatter.parse(dt)).toLong).getOrElse(0L)
  }
  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }
  def parseTaxiT(rr: RichRow, locField: String): Long = {
    rr.getAs[String](locField).map(_.toLong).getOrElse(0)
  }
  def parseTaxiPeople(rr: RichRow, peopleField: String): Long = {
    rr.getAs[String](peopleField).map(_.toLong).getOrElse(0)
  }
  def parse(row: org.apache.spark.sql.Row): Trip = {
    val rr = new RichRow(row)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude"),
      tripDistance = parseTaxiLoc(rr, "trip_distance"),
      tripTimeInSecs = parseTaxiT(rr, "trip_time_in_secs"),
      hourTime = parseTaxiTimeHour(rr, "pickup_datetime"),
      people = parseTaxiPeople(rr, "passenger_count")
    )
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }
}
class RichRow(row: org.apache.spark.sql.Row) {
  def getAs[T](field: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }
  }
}
