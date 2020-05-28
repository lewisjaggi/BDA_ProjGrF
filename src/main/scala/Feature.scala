import com.cloudera.science.geojson.RichGeometry
import spray.json._

case class Feature(
                    val id: Option[JsValue],
                    val properties: Map[String, JsValue],
                    val geometry: RichGeometry) {
  def apply(property: String) = properties(property)
  def get(property: String) = properties.get(property)

}

