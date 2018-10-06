package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._


object ChangesetStats extends CommandApp(
  name = "changeset-stats",
  header = "Changeset statistics",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsOpt =
      Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.").orNone
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, changesetsOpt, outputOpt).mapN { (historySource, changesetSource, output) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
      import spark.implicits._

      val history = spark.read.orc(historySource)

      val pointGeoms = ProcessOSM.geocode(ProcessOSM.constructPointGeometries(
        // pre-filter to nodes with tags
        history.where('type === "node" and size('tags) > 0)
      ).withColumn("minorVersion", lit(0)))

      val wayGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to ways with tags
        history.where('type === "way" and size('tags) > 0),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val augmentedWays = wayGeoms
        .withColumn("length", when(isRoad('tags) or isWaterway('tags), ST_Length('geom)).otherwise(lit(0)))
        .withColumn("delta",
          when(isRoad('tags) or isWaterway('tags),
            coalesce(abs('length - (lag('length, 1) over idByUpdated)), lit(0)))
            .otherwise(lit(0)))

      val isPark: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("leisure", "") == "park"
      }

      val hasAddress: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.contains("addr:housenumber")
      }

      val TrafficSignValues = Set("stop", "give_way")

      val isTrafficSign: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.contains("highway") && TrafficSignValues.contains(tags("highway"))
      }

      val isTrafficSignal: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "traffic_signals"
      }

      val isParking: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("amenity", "") == "parking"
      }

      val isParkingAisle: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "service" && tags.getOrElse("service", "") == "parking_aisle"
      }

      val isServiceRoad: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "service"
      }

      val isDriveway: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "service" && tags.getOrElse("service", "") == "driveway"
      }

      val hasLanes: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.contains("highway") && tags.contains("lanes")
      }

      val isSidewalk: UserDefinedFunction = udf {
        tags: Map[String, String] => (tags.getOrElse("highway", "") == "footway" && tags.getOrElse("footway", "") == "sidewalk") ||
          (tags.contains("highway") && tags.getOrElse("sidewalk", "no").toLowerCase != "no")
      }

      val isSwimmingPool: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("leisure", "") == "swimming_pool"
      }

      val isManhole: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.contains("manhole")
      }

      val isFlagpole: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("man_made", "") == "flagpole"
      }

      val isBollard: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("barrier", "") == "bollard"
      }

      val isBarrier: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.contains("barrier")
      }

      val isBikeParking: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("amenity", "") == "bicycle_parking"
      }

      val isBikePath: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "cycleway" || tags.contains("cycleway")
      }

      val isSurveillance: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("man_made", "") == "surveillance"
      }

      val isBusShelter: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("highway", "") == "bus_stop" && tags.getOrElse("shelter", "no").toLowerCase != "no"
      }

      val isVendingMachine: UserDefinedFunction = udf {
        tags: Map[String, String] => tags.getOrElse("amenity", "") == "vending_machine"
      }

      val isCrossing: UserDefinedFunction = udf {
        tags: Map[String, String] => (tags.getOrElse("highway", "") == "footway" && tags.getOrElse("footway", "") == "crossing") ||
          tags.getOrElse("highway", "") == "crossing"
      }

      val wayChangesetStats = augmentedWays
        .withColumn("road_m_added",
          when(isRoad('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("road_m_modified",
          when(isRoad('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_added",
          when(isWaterway('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("waterway_m_modified",
          when(isWaterway('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("roads_added",
          when(isRoad('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("roads_modified",
          when(isRoad('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_added",
          when(isWaterway('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_modified",
          when(isWaterway('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_added",
          when(isBuilding('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_modified",
          when(isBuilding('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_added",
          when(isPOI('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("parks_added",
          when(isPark('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("parks_modified",
          when(isPark('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("addresses_added",
          when(hasAddress('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("addresses_modified",
          when(hasAddress('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_added",
          when(isParking('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_modified",
          when(isParking('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_aisle_m_added",
          when(isParkingAisle('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("parking_aisle_m_modified",
          when(isParkingAisle('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("parking_aisles_added",
          when(isParkingAisle('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_aisles_modified",
          when(isParkingAisle('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("service_road_m_added",
          when(isServiceRoad('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("service_road_m_modified",
          when(isServiceRoad('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("service_roads_added",
          when(isServiceRoad('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("service_roads_modified",
          when(isServiceRoad('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("driveway_m_added",
          when(isDriveway('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("driveway_m_modified",
          when(isDriveway('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("driveways_added",
          when(isDriveway('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("driveways_modified",
          when(isDriveway('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("sidewalk_m_added",
        when(isSidewalk('tags) and 'version === 1 and 'minorVersion === 0, 'length)
          .otherwise(lit(0)))
        .withColumn("sidewalk_m_modified",
          when(isSidewalk('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("sidewalks_added",
          when(isSidewalk('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("sidewalks_modified",
          when(isSidewalk('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("crossings_added",
          when(isCrossing('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("crossings_modified",
          when(isCrossing('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("lane_m_added",
          when(hasLanes('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("lane_m_modified",
          when(hasLanes('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("lanes_added",
          when(hasLanes('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("lanes_modified",
          when(hasLanes('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("cycleway_m_added",
          when(isBikePath('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("cycleway_m_modified",
          when(isBikePath('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("cycleways_added",
          when(isBikePath('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("cycleways_modified",
          when(isBikePath('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("swimming_pools_added",
          when(isSwimmingPool('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("swimming_pools_modified",
          when(isSwimmingPool('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("barriers_added",
          when(isBarrier('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("barriers_modified",
          when(isBarrier('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("bike_parking_added",
          when(isBikeParking('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("bike_parking_modified",
          when(isBikeParking('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("shelters_added",
          when(isBusShelter('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("shelters_modified",
          when(isBusShelter('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset, 'uid)
        .agg(
          sum('road_m_added / 1000) as 'road_km_added,
          sum('road_m_modified / 1000) as 'road_km_modified,
          sum('waterway_m_added / 1000) as 'waterway_km_added,
          sum('waterway_m_modified / 1000) as 'waterway_km_modified,
          sum('roads_added) as 'roads_added,
          sum('roads_modified) as 'roads_modified,
          sum('waterways_added) as 'waterways_added,
          sum('waterways_modified) as 'waterways_modified,
          sum('buildings_added) as 'buildings_added,
          sum('buildings_modified) as 'buildings_modified,
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          sum('parks_added) as 'parks_added,
          sum('parks_modified) as 'parks_modified,
          sum('addresses_added) as 'addresses_added,
          sum('addresses_modified) as 'addresses_modified,
          sum('parking_added) as 'parking_added,
          sum('parking_modified) as 'parking_modified,
          sum('parking_aisle_m_added / 1000) as 'parking_aisle_km_added,
          sum('parking_aisle_m_modified / 1000) as 'parking_aisle_km_modified,
          sum('parking_aisles_added) as 'parking_aisles_added,
          sum('parking_aisles_modified) as 'parking_aisles_modified,
          sum('service_road_m_added / 1000) as 'service_road_km_added,
          sum('service_road_m_modified / 1000) as 'service_road_km_modified,
          sum('service_roads_added) as 'service_roads_added,
          sum('service_roads_modified) as 'service_roads_modified,
          sum('driveway_m_added / 1000) as 'driveway_km_added,
          sum('driveway_m_modified / 1000) as 'driveway_km_modified,
          sum('driveways_added) as 'driveways_added,
          sum('driveways_modified) as 'driveways_modified,
          sum('sidewalk_m_added / 1000) as 'sidewalk_km_added,
          sum('sidewalk_m_modified / 1000) as 'sidewalk_km_modified,
          sum('sidewalks_added) as 'sidewalks_added,
          sum('sidewalks_modified) as 'sidewalks_modified,
          sum('crossings_added) as 'crossings_added,
          sum('crossings_modified) as 'crossings_modified,
          sum('lane_m_added / 1000) as 'lane_km_added,
          sum('lane_m_modified / 1000) as 'lane_km_modified,
          sum('lanes_added) as 'lanes_added,
          sum('lanes_modified) as 'lanes_modified,
          sum('swimming_pools_added) as 'swimming_pools_added,
          sum('swimming_pools_modified) as 'swimming_pools_modified,
          sum('barriers_added) as 'barriers_added,
          sum('barriers_modified) as 'barriers_modified,
          sum('bike_parking_added) as 'bike_parking_added,
          sum('bike_parking_modified) as 'bike_parking_modified,
          sum('cycleway_m_added / 1000) as 'cycleway_km_added,
          sum('cycleway_m_modified / 1000) as 'cycleway_km_modified,
          sum('cycleways_added) as 'cycleways_added,
          sum('cycleways_modified) as 'cycleways_modified,
          sum('shelters_added) as 'shelters_added,
          sum('shelters_modified) as 'shelters_modified
//          count_values(flatten(collect_list('countries))) as 'countries
        )

      val pointChangesetStats = pointGeoms
        .withColumn("pois_added",
          when(isPOI('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("parks_added",
          when(isPark('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("parks_modified",
          when(isPark('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("addresses_added",
          when(hasAddress('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("addresses_modified",
          when(hasAddress('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("traffic_signs_added",
          when(isTrafficSign('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("traffic_signs_modified",
          when(isTrafficSign('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("traffic_signals_added",
          when(isTrafficSignal('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("traffic_signals_modified",
          when(isTrafficSignal('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_added",
          when(isParking('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("parking_modified",
          when(isParking('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("swimming_pools_added",
          when(isSwimmingPool('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("swimming_pools_modified",
          when(isSwimmingPool('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("manholes_added",
          when(isManhole('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("manholes_modified",
          when(isManhole('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("flagpoles_added",
          when(isFlagpole('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("flagpoles_modified",
          when(isFlagpole('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("bollards_added",
          when(isBollard('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("bollards_modified",
          when(isBollard('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("barriers_added",
          when(isBarrier('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("barriers_modified",
          when(isBarrier('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("bike_parking_added",
          when(isBikeParking('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("bike_parking_modified",
          when(isBikeParking('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("surveillance_added",
          when(isSurveillance('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("surveillance_modified",
          when(isSurveillance('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("shelters_added",
          when(isBusShelter('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("shelters_modified",
          when(isBusShelter('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("vending_machines_added",
          when(isVendingMachine('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("vending_machines_modified",
          when(isVendingMachine('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("crossings_added",
          when(isCrossing('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("crossings_modified",
          when(isCrossing('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset, 'uid)
        .agg(
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          sum('parks_added) as 'parks_added,
          sum('parks_modified) as 'parks_modified,
          sum('addresses_added) as 'addresses_added,
          sum('addresses_modified) as 'addresses_modified,
          sum('traffic_signs_added) as 'traffic_signs_added,
          sum('traffic_signs_modified) as 'traffic_signs_modified,
          sum('traffic_signals_added) as 'traffic_signals_added,
          sum('traffic_signals_modified) as 'traffic_signals_modified,
          sum('parking_added) as 'parking_added,
          sum('parking_modified) as 'parking_modified,
          sum('swimming_pools_added) as 'swimming_pools_added,
          sum('swimming_pools_modified) as 'swimming_pools_modified,
          sum('manholes_added) as 'manholes_added,
          sum('manholes_modified) as 'manholes_modified,
          sum('flagpoles_added) as 'flagpoles_added,
          sum('flagpoles_modified) as 'flagpoles_modified,
          sum('bollards_added) as 'bollards_added,
          sum('bollards_modified) as 'bollards_modified,
          sum('barriers_added) as 'barriers_added,
          sum('barriers_modified) as 'barriers_modified,
          sum('bike_parking_added) as 'bike_parking_added,
          sum('bike_parking_modified) as 'bike_parking_modified,
          sum('surveillance_added) as 'surveillance_added,
          sum('surveillance_modified) as 'surveillance_modified,
          sum('shelters_added) as 'shelters_added,
          sum('shelters_modified) as 'shelters_modified,
          sum('vending_machines_added) as 'vending_machines_added,
          sum('vending_machines_modified) as 'vending_machines_modified,
          sum('crossings_added) as 'crossings_added,
          sum('crossings_modified) as 'crossings_modified
//          count_values(flatten(collect_list('countries))) as 'countries
        )

      // coalesce values to deal with nulls introduced by the outer join
      val rawChangesetStats = wayChangesetStats
        .withColumnRenamed("pois_added", "way_pois_added")
        .withColumnRenamed("pois_modified", "way_pois_modified")
        .withColumnRenamed("parks_added", "way_parks_added")
        .withColumnRenamed("parks_modified", "way_parks_modified")
        .withColumnRenamed("addresses_added", "way_addresses_added")
        .withColumnRenamed("addresses_modified", "way_addresses_modified")
        .withColumnRenamed("parking_added", "way_parking_added")
        .withColumnRenamed("parking_modified", "way_parking_modified")
        .withColumnRenamed("swimming_pools_added", "way_swimming_pools_added")
        .withColumnRenamed("swimming_pools_modified", "way_swimming_pools_modified")
        .withColumnRenamed("barriers_added", "way_barriers_added")
        .withColumnRenamed("barriers_modified", "way_barriers_modified")
        .withColumnRenamed("bike_parking_added", "way_bike_parking_added")
        .withColumnRenamed("bike_parking_modified", "way_bike_parking_modified")
        .withColumnRenamed("shelters_added", "way_shelters_added")
        .withColumnRenamed("shelters_modified", "way_shelters_modified")
        .withColumnRenamed("crossings_added", "way_crossings_added")
        .withColumnRenamed("crossings_modified", "way_crossings_modified")
//        .withColumnRenamed("countries", "way_countries")
        .join(pointChangesetStats
          .withColumnRenamed("pois_added", "node_pois_added")
          .withColumnRenamed("pois_modified", "node_pois_modified")
          .withColumnRenamed("parks_added", "node_parks_added")
          .withColumnRenamed("parks_modified", "node_parks_modified")
          .withColumnRenamed("addresses_added", "node_addresses_added")
          .withColumnRenamed("addresses_modified", "node_addresses_modified")
          .withColumnRenamed("parking_added", "node_parking_added")
          .withColumnRenamed("parking_modified", "node_parking_modified")
          .withColumnRenamed("swimming_pools_added", "node_swimming_pools_added")
          .withColumnRenamed("swimming_pools_modified", "node_swimming_pools_modified")
          .withColumnRenamed("barriers_added", "node_barriers_added")
          .withColumnRenamed("barriers_modified", "node_barriers_modified")
          .withColumnRenamed("bike_parking_added", "node_bike_parking_added")
          .withColumnRenamed("bike_parking_modified", "node_bike_parking_modified")
          .withColumnRenamed("shelters_added", "node_shelters_added")
          .withColumnRenamed("shelters_modified", "node_shelters_modified")
          .withColumnRenamed("crossings_added", "node_crossings_added")
          .withColumnRenamed("crossings_modified", "node_crossings_modified"),
//          .withColumnRenamed("countries", "node_countries"),
          Seq("uid", "changeset"),
          "full_outer")
        .withColumn("road_km_added", coalesce('road_km_added, lit(0)))
        .withColumn("road_km_modified", coalesce('road_km_modified, lit(0)))
        .withColumn("waterway_km_added", coalesce('waterway_km_added, lit(0)))
        .withColumn("waterway_km_modified", coalesce('waterway_km_modified, lit(0)))
        .withColumn("roads_added", coalesce('roads_added, lit(0)))
        .withColumn("roads_modified", coalesce('roads_modified, lit(0)))
        .withColumn("waterways_added", coalesce('waterways_added, lit(0)))
        .withColumn("waterways_modified", coalesce('waterways_modified, lit(0)))
        .withColumn("buildings_added", coalesce('buildings_added, lit(0)))
        .withColumn("buildings_modified", coalesce('buildings_modified, lit(0)))
        .withColumn("parking_aisle_km_added", coalesce('parking_aisle_km_added, lit(0)))
        .withColumn("parking_aisle_km_modified", coalesce('parking_aisle_km_modified, lit(0)))
        .withColumn("parking_aisles_added", coalesce('parking_aisles_added, lit(0)))
        .withColumn("parking_aisles_modified", coalesce('parking_aisles_modified, lit(0)))
        .withColumn("service_road_km_added", coalesce('service_road_km_added, lit(0)))
        .withColumn("service_road_km_modified", coalesce('service_road_km_modified, lit(0)))
        .withColumn("service_roads_added", coalesce('service_roads_added, lit(0)))
        .withColumn("service_roads_modified", coalesce('service_roads_modified, lit(0)))
        .withColumn("driveway_km_added", coalesce('driveway_km_added, lit(0)))
        .withColumn("driveway_km_modified", coalesce('driveway_km_modified, lit(0)))
        .withColumn("driveways_added", coalesce('driveways_added, lit(0)))
        .withColumn("driveways_modified", coalesce('driveways_modified, lit(0)))
        .withColumn("sidewalk_km_added", coalesce('sidewalk_km_added, lit(0)))
        .withColumn("sidewalk_km_modified", coalesce('sidewalk_km_modified, lit(0)))
        .withColumn("sidewalks_added", coalesce('sidewalks_added, lit(0)))
        .withColumn("sidewalks_modified", coalesce('sidewalks_modified, lit(0)))
        .withColumn("lane_km_added", coalesce('lane_km_added, lit(0)))
        .withColumn("lane_km_modified", coalesce('lane_km_modified, lit(0)))
        .withColumn("lanes_added", coalesce('lanes_added, lit(0)))
        .withColumn("lanes_modified", coalesce('lanes_modified, lit(0)))
        .withColumn("cycleway_km_added", coalesce('cycleway_km_added, lit(0)))
        .withColumn("cycleway_km_modified", coalesce('cycleway_km_modified, lit(0)))
        .withColumn("cycleways_added", coalesce('cycleways_added, lit(0)))
        .withColumn("cycleways_modified", coalesce('cycleways_modified, lit(0)))
        .withColumn("traffic_signs_added", coalesce('traffic_signs_added, lit(0)))
        .withColumn("traffic_signs_modified", coalesce('traffic_signs_modified, lit(0)))
        .withColumn("traffic_signals_added", coalesce('traffic_signals_added, lit(0)))
        .withColumn("traffic_signals_modified", coalesce('traffic_signals_modified, lit(0)))
        .withColumn("manholes_added", coalesce('manholes_added, lit(0)))
        .withColumn("manholes_modified", coalesce('manholes_modified, lit(0)))
        .withColumn("flagpoles_added", coalesce('flagpoles_added, lit(0)))
        .withColumn("flagpoles_modified", coalesce('flagpoles_modified, lit(0)))
        .withColumn("bollards_added", coalesce('bollards_added, lit(0)))
        .withColumn("bollards_modified", coalesce('bollards_modified, lit(0)))
        .withColumn("surveillance_added", coalesce('surveillance_added, lit(0)))
        .withColumn("surveillance_modified", coalesce('surveillance_modified, lit(0)))
        .withColumn("vending_machines_added", coalesce('vending_machines_added, lit(0)))
        .withColumn("vending_machines_modified", coalesce('vending_machines_modified, lit(0)))
        .withColumn("pois_added",
          coalesce('way_pois_added, lit(0)) + coalesce('node_pois_added, lit(0)))
        .withColumn("pois_modified",
          coalesce('way_pois_modified, lit(0)) + coalesce('node_pois_modified, lit(0)))
        .withColumn("parks_added",
          coalesce('way_parks_added, lit(0)) + coalesce('node_parks_added, lit(0)))
        .withColumn("parks_modified",
          coalesce('way_parks_modified, lit(0)) + coalesce('node_parks_modified, lit(0)))
        .withColumn("addresses_added",
          coalesce('way_addresses_added, lit(0)) + coalesce('node_addresses_added, lit(0)))
        .withColumn("addresses_modified",
          coalesce('way_addresses_modified, lit(0)) + coalesce('node_addresses_modified, lit(0)))
        .withColumn("parking_added",
          coalesce('way_parking_added, lit(0)) + coalesce('node_parking_added, lit(0)))
        .withColumn("parking_modified",
          coalesce('way_parking_modified, lit(0)) + coalesce('node_parking_modified, lit(0)))
        .withColumn("swimming_pools_added",
          coalesce('way_swimming_pools_added, lit(0)) + coalesce('node_swimming_pools_added, lit(0)))
        .withColumn("swimming_pools_modified",
          coalesce('way_swimming_pools_modified, lit(0)) + coalesce('node_swimming_pools_modified, lit(0)))
        .withColumn("barriers_added",
          coalesce('way_barriers_added, lit(0)) + coalesce('node_barriers_added, lit(0)))
        .withColumn("barriers_modified",
          coalesce('way_barriers_modified, lit(0)) + coalesce('node_barriers_modified, lit(0)))
        .withColumn("bike_parking_added",
          coalesce('way_bike_parking_added, lit(0)) + coalesce('node_bike_parking_added, lit(0)))
        .withColumn("bike_parking_modified",
          coalesce('way_bike_parking_modified, lit(0)) + coalesce('node_bike_parking_modified, lit(0)))
        .withColumn("shelters_added",
          coalesce('way_shelters_added, lit(0)) + coalesce('node_shelters_added, lit(0)))
        .withColumn("shelters_modified",
          coalesce('way_shelters_modified, lit(0)) + coalesce('node_shelters_modified, lit(0)))
        .withColumn("crossings_added",
          coalesce('way_crossings_added, lit(0)) + coalesce('node_crossings_added, lit(0)))
        .withColumn("crossings_modified",
          coalesce('way_crossings_modified, lit(0)) + coalesce('node_crossings_modified, lit(0)))
//        .withColumn("countries", merge_counts('node_countries, 'way_countries))
        .drop('way_pois_added)
        .drop('node_pois_added)
        .drop('way_pois_modified)
        .drop('node_pois_modified)
        .drop('way_parks_added)
        .drop('node_parks_added)
        .drop('way_parks_modified)
        .drop('node_parks_modified)
        .drop('way_addresses_added)
        .drop('node_addresses_added)
        .drop('way_addresses_modified)
        .drop('node_addresses_modified)
        .drop('way_parking_added)
        .drop('node_parking_added)
        .drop('way_parking_modified)
        .drop('node_parking_modified)
        .drop('way_swimming_pools_added)
        .drop('node_swimming_pools_added)
        .drop('way_swimming_pools_modified)
        .drop('node_swimming_pools_modified)
        .drop('way_barriers_added)
        .drop('node_barriers_added)
        .drop('way_barriers_modified)
        .drop('node_barriers_modified)
        .drop('way_bike_parking_added)
        .drop('node_bike_parking_added)
        .drop('way_bike_parking_modified)
        .drop('node_bike_parking_modified)
        .drop('way_shelters_added)
        .drop('node_shelters_added)
        .drop('way_shelters_modified)
        .drop('node_shelters_modified)
        .drop('way_crossings_added)
        .drop('node_crossings_added)
        .drop('way_crossings_modified)
        .drop('node_crossings_modified)
//        .drop('way_countries)
//        .drop('node_countries)

//      val changesets = spark.read.orc(changesetSource)
//
//      val changesetMetadata = changesets
//        .select(
//          'id as 'changeset,
//          'uid,
//          'user as 'name,
//          'tags.getItem("created_by") as 'editor,
//          'created_at,
//          'closed_at,
//          hashtags('tags) as 'hashtags
//        )

      val changesetStats = rawChangesetStats
//        .join(changesetMetadata, Seq("changeset"), "left_outer")
        .cache

      changesetStats
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changeset-stats").toString)

      val userStats = changesetStats
        .groupBy('uid)
        .agg(
          sum('road_km_added) as 'road_km_added,
          sum('road_km_modified) as 'road_km_modified,
          sum('waterway_km_added) as 'waterway_km_added,
          sum('waterway_km_modified) as 'waterway_km_modified,
          sum('roads_added) as 'roads_added,
          sum('roads_modified) as 'roads_modified,
          sum('waterways_added) as 'waterways_added,
          sum('waterways_modified) as 'waterways_modified,
          sum('buildings_added) as 'buildings_added,
          sum('buildings_modified) as 'buildings_modified,
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          sum('parking_aisle_km_added) as 'parking_aisle_km_added,
          sum('parking_aisle_km_modified) as 'parking_aisle_km_modified,
          sum('parking_aisles_added) as 'parking_aisles_added,
          sum('parking_aisles_modified) as 'parking_aisles_modified,
          sum('service_road_km_added) as 'service_road_km_added,
          sum('service_road_km_modified) as 'service_road_km_modified,
          sum('service_roads_added) as 'service_roads_added,
          sum('service_roads_modified) as 'service_roads_modified,
          sum('driveway_km_added) as 'driveway_km_added,
          sum('driveway_km_modified) as 'driveway_km_modified,
          sum('driveways_added) as 'driveways_added,
          sum('driveways_modified) as 'driveways_modified,
          sum('sidewalk_km_added) as 'sidewalk_km_added,
          sum('sidewalk_km_modified) as 'sidewalk_km_modified,
          sum('sidewalks_added) as 'sidewalks_added,
          sum('sidewalks_modified) as 'sidewalks_modified,
          sum('lane_km_added) as 'lane_km_added,
          sum('lane_km_modified) as 'lane_km_modified,
          sum('lanes_added) as 'lanes_added,
          sum('lanes_modified) as 'lanes_modified,
          sum('cycleway_km_added) as 'cycleway_km_added,
          sum('cycleway_km_modified) as 'cycleway_km_modified,
          sum('cycleways_added) as 'cycleways_added,
          sum('cycleways_modified) as 'cycleways_modified,
          sum('parks_added) as 'parks_added,
          sum('parks_modified) as 'parks_modified,
          sum('addresses_added) as 'addresses_added,
          sum('addresses_modified) as 'addresses_modified,
          sum('traffic_signs_added) as 'traffic_signs_added,
          sum('traffic_signs_modified) as 'traffic_signs_modified,
          sum('traffic_signals_added) as 'traffic_signals_added,
          sum('traffic_signals_modified) as 'traffic_signals_modified,
          sum('parking_added) as 'parking_added,
          sum('parking_modified) as 'parking_modified,
          sum('swimming_pools_added) as 'swimming_pools_added,
          sum('swimming_pools_modified) as 'swimming_pools_modified,
          sum('manholes_added) as 'manholes_added,
          sum('manholes_modified) as 'manholes_modified,
          sum('flagpoles_added) as 'flagpoles_added,
          sum('flagpoles_modified) as 'flagpoles_modified,
          sum('bollards_added) as 'bollards_added,
          sum('bollards_modified) as 'bollards_modified,
          sum('barriers_added) as 'barriers_added,
          sum('barriers_modified) as 'barriers_modified,
          sum('bike_parking_added) as 'bike_parking_added,
          sum('bike_parking_modified) as 'bike_parking_modified,
          sum('surveillance_added) as 'surveillance_added,
          sum('surveillance_modified) as 'surveillance_modified,
          sum('shelters_added) as 'shelters_added,
          sum('shelters_modified) as 'shelters_modified,
          sum('vending_machines_added) as 'vending_machines_added,
          sum('vending_machines_modified) as 'vending_machines_modified,
          sum('crossings_added) as 'crossings_added,
          sum('crossings_modified) as 'crossings_modified,
          count('changeset) as 'changeset_count
          // TODO more efficient as a UDAF; even more efficient using mapPartitions
//          count_values(collect_list('editor)) as 'editors,
//          count_values(collect_list(to_date(date_trunc("day", 'created_at)))) as 'edit_times,
//          count_values(flatten(collect_list('hashtags))) as 'hashtags,
//          sum_counts(collect_list('countries)) as 'countries
        )

      userStats
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("user-stats").toString)

      spark.stop()
    }
  }
)

