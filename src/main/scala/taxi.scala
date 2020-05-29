import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.cloudera.science.geojson.FeatureCollection
import org.apache.spark.sql._

object taxi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("My App")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val taxiRaw = spark.read.format("csv").option("header", "true") .load("taxidata\\1Mdata.csv")

    //taxiRaw = taxiRaw.map[RichRow](x => new RichRow(x))

    import spark.implicits._
    val safeParse = safe(parse)
    val taxiParsed = taxiRaw.rdd.map(safeParse)
    //taxiParsed.map(_.isLeft).countByValue().foreach(println)


    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val taxiGood = taxiParsed.map(_.left.get).toDF()
    taxiGood.cache()
    taxiGood.show(100)

    println("END")

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
    //taxiRaw.show()
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
