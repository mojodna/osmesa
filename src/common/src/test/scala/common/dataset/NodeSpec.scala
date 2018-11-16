package common.dataset

import com.vividsolutions.jts.geom.Point
import common.TestEnvironment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec
import osmesa.common.traits._
import osmesa.common.{traits, _}

class NodeSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Node]") {
    import implicits._

    val rawNodes: Dataset[Node with Timestamp] with History = asHistory(HistoryDF).nodes
    val nodesWithValidity: Dataset[Node with Validity] with History = rawNodes.withValidity
    val nodes = nodesWithValidity.asInstanceOf[Dataset[Node]]

    describe("asPoints") {
      val geoms: Dataset[OSMFeature[Point] with Metadata with Visibility] = nodes.asPoints.cache
      val geom = geoms.first()

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))

        assert(geom.isInstanceOf[Geometry[Point]])
      }

      it("should include VersionControl") {
        assert(geoms.schema.fieldNames.contains("changeset"))

        assert(geom.isInstanceOf[VersionControl])
      }

      it("should not include Validity") {
        assert(!geom.isInstanceOf[Validity])
      }

      it("should not include Timestamp") {
        assert(!geoms.schema.fieldNames.contains("timestamp"))

        assert(!geom.isInstanceOf[traits.Timestamp])
      }

      it("should produce the correct number of results") {
        assert(geoms.count === 33533)
      }

      it("should be distinct by changeset") {
        assert(geoms.count === geoms.groupBy('id, 'changeset).agg(first('id)).count)
      }
    }
  }
}