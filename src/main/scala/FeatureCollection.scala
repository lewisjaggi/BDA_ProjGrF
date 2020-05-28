import spray.json.JsValue
import com.cloudera.science.geojson.RichGeometry


case class FeatureCollection(features: Array[Feature])
  extends IndexedSeq[Feature] {
  def apply(index: Int) = features(index)
  def length = features.length






}

