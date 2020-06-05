import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.cloudera.science.geojson.{Feature, FeatureCollection}
import org.apache.spark.sql._
import org.apache.spark._

object taxi {
  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    val spark = SparkSession.builder
      .master("local[*]")
      .appName("My App")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    val taxiRaw = spark.read.format("csv").option("header", "true") .load("taxidata\\1Mdata.csv")

    //taxiRaw = taxiRaw.map[RichRow](x => new RichRow(x))

    import spark.implicits._
    val safeParse = safe(parse)
    val taxiParsed = taxiRaw.rdd.map(safeParse)
    //taxiParsed.map(_.isLeft).countByValue().foreach(println)


    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val taxiGood = taxiParsed.map(_.left.getOrElse(Trip("",0,0,0,0,0,0))).toDS()
    taxiGood.cache()
    taxiGood.show(100)


    import org.apache.spark.sql.functions.udf
    val hoursUDF = udf(hours)
    /*taxiGood.
      groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
      count().
      sort("h").
      show()*/


    val geojson = scala.io.Source.
      fromFile("taxidata/nyc-boroughs.geojson").
      mkString

    import com.cloudera.science.geojson
    import com.cloudera.science.geojson.GeoJsonProtocol._
    import spray.json._

    val features = geojson.parseJson.convertTo[FeatureCollection]

    import com.esri.core.geometry.Point
    val p = new Point(-73.994499, 40.75066)
    val borough = features.find(f => f.geometry.contains(p))
    println(borough)

    import org.apache.spark.sql.functions.udf

    taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
      count().
      sort("h").
      show()

    taxiGood.where(hoursUDF($"pickupTime", $"dropoffTime") < 0).
      collect().
      foreach(println)

    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where(
      "hours(pickupTime, dropoffTime) BETWEEN 0 AND 3"
    )

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
    taxiClean.
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
      show()

    val sessions = taxiDone.
      repartition($"license").
      sortWithinPartitions($"license", $"pickupTime")
    sessions.cache()

    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

    import org.apache.spark.sql.functions._

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
      show()

  }



  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }
  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
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
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
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
