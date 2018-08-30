package osmesa.elasticsearch

import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import osmesa.common.functions._

object Ingest
    extends CommandApp(
      name = "elasticsearch-ingest",
      header = "Ingest geometries from an ORC file",
      main = {
        val orcO = Opts
          .option[String]("orc", help = "Location of the ORC file to process")

        (orcO).map {
          (orc) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("elasticsearch-ingest")
              .set(
                "spark.serializer",
                classOf[org.apache.spark.serializer.KryoSerializer].getName
              )
              .set(
                "spark.kryo.registrator",
                classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName
              )

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            val geoms = ss.read.orc(orc)

            // curl -X DELETE "localhost:9200/spark"
            // curl -X PUT "localhost:9200/spark" -H 'Content-Type: application/json' -d @settings.json
            // curl -X PUT "localhost:9200/spark/_mapping/ri-geoms" -H 'Content-Type: application/json' -d @mapping.json

            geoms
              .withColumn(
                "guid",
                concat(
                  '_type,
                  lit("/"),
                  'id,
                  lit("@"),
                  'version,
                  lit("."),
                  'minorVersion
                )
              )
              .withColumn("wkt", ST_AsText('geom))
              .drop('geom)
              .withColumnRenamed("_type", "type")
              .saveToEs(
                "spark/ri-geoms",
                Map(
                  "es.mapping.id" -> "guid",
//                  "es.index.mapping.depth.limit" -> "1",
//                  "index.mapping.total_fields.limit" -> "10000",
                  "es.index.auto.create" -> "false"
                )
              )
        }
      }
    )
