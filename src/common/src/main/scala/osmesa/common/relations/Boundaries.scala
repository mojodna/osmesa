package osmesa.common.relations
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.Logger

object Boundaries {
  private lazy val logger = Logger.getLogger(getClass)

  def build(id: Long,
            version: Int,
            timestamp: Timestamp,
            types: Seq[Byte],
            roles: Seq[String],
            _geoms: Seq[Geometry]): Option[Geometry] = ???
}
