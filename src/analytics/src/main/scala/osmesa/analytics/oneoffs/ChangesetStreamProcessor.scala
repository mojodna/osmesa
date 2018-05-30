package osmesa.analytics.oneoffs

import java.net.URI
import java.sql.{Connection, DriverManager, Timestamp}

import cats.implicits._
import com.monovore.decline._
import org.apache.spark._
import org.apache.spark.sql._
import osmesa.common.functions.osm._

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangesetStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --database-url $DATABASE_URL
 */
object ChangesetStreamProcessor
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))
        val databaseUrlOpt = Opts
          .option[URI]("database-url", short = "d", metavar = "database URL", help = "Database URL")
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val endSequenceOpt = Opts
          .option[Int]("end-sequence",
                       short = "e",
                       metavar = "sequence",
                       help = "Ending sequence. If absent, this will be an infinite stream.")
          .orNone

        (changesetSourceOpt, databaseUrlOpt, startSequenceOpt, endSequenceOpt).mapN {
          (changesetSource, databaseUri, startSequence, endSequence) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("changeset-stream-processor")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            val options = Map("base_uri" -> changesetSource.toString) ++
              startSequence
                .map(s => Map("start_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map("end_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changesets =
              ss.readStream
                .format("changesets")
                .options(options)
                .load

            val changesetProcessor = changesets
              .select('id,
                      'created_at,
                      'closed_at,
                      'user,
                      'uid,
                      'tags.getField("created_by") as 'editor,
                      hashtags('tags) as 'hashtags)
              .writeStream
              .queryName("update changeset metadata")
              .foreach(new ForeachWriter[Row] {
                var partitionId: Long = _
                var version: Long = _
                var connection: Connection = _
                // https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql
                val GetHashtagIdQuery: String =
                  """
                    |WITH data AS (
                    |  SELECT
                    |    ? AS hashtag
                    |),
                    |ins AS (
                    |  INSERT INTO hashtags AS h (
                    |    hashtag
                    |  ) SELECT * FROM data
                    |  ON CONFLICT DO NOTHING
                    |  RETURNING id
                    |)
                    |SELECT id
                    |FROM ins
                    |UNION ALL
                    |SELECT id
                    |FROM data
                    |JOIN hashtags USING(hashtag)
                  """.stripMargin

                val UpdateChangesetsQuery: String =
                  """
                    |-- pre-shape the data to avoid repetition
                    |WITH data AS (
                    |  SELECT
                    |    ? AS id,
                    |    ? AS editor,
                    |    ? AS user_id,
                    |    ?::timestamp with time zone AS created_at,
                    |    ?::timestamp with time zone AS closed_at,
                    |    current_timestamp AS updated_at
                    |)
                    |INSERT INTO changesets AS c (
                    |  id,
                    |  editor,
                    |  user_id,
                    |  created_at,
                    |  closed_at,
                    |  updated_at
                    |) SELECT * FROM data
                    |ON CONFLICT (id) DO UPDATE
                    |SET
                    |  editor = EXCLUDED.editor,
                    |  user_id = EXCLUDED.user_id,
                    |  created_at = EXCLUDED.created_at,
                    |  closed_at = EXCLUDED.closed_at,
                    |  updated_at = current_timestamp
                    |WHERE c.id = EXCLUDED.id
                  """.stripMargin

                val UpdateChangesetsHashtagsQuery: String =
                  """
                    |WITH data AS (
                    |  SELECT
                    |    ? AS changeset_id,
                    |    ? AS hashtag_id
                    |)
                    |INSERT INTO changesets_hashtags (
                    |  changeset_id,
                    |  hashtag_id
                    |) SELECT * FROM data
                    |ON CONFLICT DO NOTHING
                  """.stripMargin

                val UpdateUsersQuery: String =
                  """
                    |--pre-shape the data to avoid repetition
                    |WITH data AS (
                    |  SELECT
                    |    ? AS id,
                    |    ? AS name
                    |)
                    |INSERT INTO users AS u (
                    |  id,
                    |  name
                    |) SELECT * FROM data
                    |ON CONFLICT (id) DO UPDATE
                    |-- update the user's name if necessary
                    |SET
                    |  name = EXCLUDED.name
                    |WHERE u.id = EXCLUDED.id
                  """.stripMargin

                def open(partitionId: Long, version: Long): Boolean = {
                  // Called when starting to process one partition of new data in the executor. The version is for data
                  // deduplication when there are failures. When recovering from a failure, some data may be generated
                  // multiple times but they will always have the same version.
                  //
                  //If this method finds using the partitionId and version that this partition has already been processed,
                  // it can return false to skip the further data processing. However, close still will be called for
                  // cleaning up resources.

                  this.partitionId = partitionId
                  this.version = version

                  connection = DriverManager.getConnection(s"jdbc:${databaseUri.toString}")

                  true
                }

                def process(row: Row): Unit = {
                  val id = row.getAs[Long]("id")
                  val createdAt = row.getAs[Timestamp]("created_at")
                  val closedAt = row.getAs[Timestamp]("closed_at")
                  val user = row.getAs[String]("user")
                  val uid = row.getAs[Long]("uid")
                  val editor = row.getAs[String]("editor")
                  val hashtags = row.getAs[Seq[String]]("hashtags")

                  val updateChangesets = connection.prepareStatement(UpdateChangesetsQuery)

                  try {
                    updateChangesets.setLong(1, id)
                    updateChangesets.setString(2, editor)
                    updateChangesets.setLong(3, uid)
                    updateChangesets.setTimestamp(4, createdAt)
                    updateChangesets.setTimestamp(5, closedAt)

                    updateChangesets.execute
                  } finally {
                    updateChangesets.close()
                  }

                  val updateUsers = connection.prepareStatement(UpdateUsersQuery)

                  try {
                    updateUsers.setLong(1, uid)
                    updateUsers.setString(2, user)

                    updateUsers.execute
                  } finally {
                    updateUsers.close()
                  }

                  hashtags.foreach {
                    hashtag =>
                      val getHashtagId = connection.prepareStatement(GetHashtagIdQuery)

                      try {
                        getHashtagId.setString(1, hashtag)

                        val rs = getHashtagId.executeQuery()

                        while (rs.next()) {
                          val hashtagId = rs.getLong("id")

                          val updateChangesetsHashtags =
                            connection.prepareStatement(UpdateChangesetsHashtagsQuery)

                          try {
                            updateChangesetsHashtags.setLong(1, id)
                            updateChangesetsHashtags.setLong(2, hashtagId)

                            updateChangesetsHashtags.execute
                          } finally {
                            updateChangesetsHashtags.close()
                          }
                        }
                      } finally {
                        getHashtagId.close()
                      }
                  }
                }

                def close(errorOrNull: Throwable): Unit = {
                  connection.close()
                }
              })
              .start

            changesetProcessor.awaitTermination()

            ss.stop()
        }
      }
    )