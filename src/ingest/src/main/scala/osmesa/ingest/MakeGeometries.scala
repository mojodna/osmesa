package osmesa

import cats.implicits._
import com.monovore.decline._
import org.apache.spark._
import org.apache.spark.sql._
import osmesa.common.ProcessOSM

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MakeGeometries \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --out=$HOME/data/osm/isle-of-man-geoms.orc \
 */

object MakeGeometries
    extends CommandApp(
      name = "osmesa-make-geometries",
      header = "Create geometries from an ORC file",
      main = {

        /* CLI option handling */
        val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
        val outO = Opts.option[String]("out", help = "ORC file containing geometries")
        val numPartitionsO =
          Opts.option[Int]("partitions", help = "Number of partitions to generate").withDefault(1)

        (orcO, outO, numPartitionsO).mapN {
          (orc, out, numPartitions) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("make-geometries")
              .set("spark.ui.showConsoleProgress", "true")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
            // for this dataset, these actually reduce performance; perhaps something to do with parsing / predicate
            // pushdown for complex types (array<struct<type:string,ref:long,role:string>> appears to be the worst offender)
//        .set("spark.sql.orc.impl", "native")
        .set("spark.sql.orc.filterPushdown", "true")

            val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            ss.sparkContext.setCheckpointDir("/tmp")

            val df = ss.read.orc(orc).repartition()

            val geoms = ProcessOSM
              .constructGeometries(df)
              .repartition(numPartitions)

            geoms.explain(true)

            geoms
              .write
              .mode(SaveMode.Overwrite)
              .orc(out)

            ss.stop()

            println("Done.")
        }
      }
    )
