package comgeAviation.scala

// Imported required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object local {
  def main(args: Array[String]): Unit = {
    // Created sparksession with hive support
    val spark:SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate();
    import spark.implicits._
    var sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    // We created a table of 20 rows for sub_part1 2018-08
    //var metadf = spark.sql("select * from default.raw_metadata_in_time_series_20rows");//var metadf = spark.sql("select * from default.raw_metadata_in_time_series_20rows");

    val metadf = spark.sql("select * from default.raw_metadata_in_time_series where unique_id = 319507")
    //metadf.show()

    // First, grouped by "adi_flight_record_number", "ems_system_id", "parameter_id", "unique_id"
    // and then selected "adi_flight_record_number", "ems_system_id", "parameter_id", "unique_id"
    val metadf2 = metadf.groupBy("adi_flight_record_number", "ems_system_id", "parameter_id", "unique_id").agg(max("ems_system_id").as("toDelete")).select("adi_flight_record_number", "ems_system_id", "parameter_id", "unique_id")
    //metadf2.show()

    //val metabefore = metadf.join(metadf2, Seq("adi_flight_record_number", "ems_system_id", "parameter_id", "unique_id"), "inner").select("unique_id", "adi_flight_record_number", "ems_system_id", "parameter_id")
    //metabefore.show()

    //var bigintdf = spark.sql("select * from default.raw_bigint_in_time_series_20rows");
    // We created a table of 20 rows for sub_part1 2018-08
    val bigintdf = spark.sql("select * from default.raw_bigint_in_time_series where unique_id = 319507")
    //bigintdf.show()

    // meta and bigint over "unique_id", "ems_system_id", "parameter_id"
    val metabigintbeforegrouped = bigintdf.join(metadf2, Seq("unique_id", "ems_system_id", "parameter_id"), "inner")
    //metabigintbeforegrouped.show()

    // final wanted table
    val metabigintgrouped = metabigintbeforegrouped.groupBy("adi_flight_record_number", "ems_system_id", "parameter_id", "value").agg(max("offset").as("MaxOffset")).select("adi_flight_record_number", "ems_system_id", "parameter_id", "value", "MaxOffset")
    //metabigintgrouped.write.saveAsTable("default.totopic_res_unique_id319507_sub_part2018_08")

    // table schema
    val metabigintgroupedSchema = StructType(Array(
      StructField("adi_flight_record_number", IntegerType),
      StructField("ems_system_id", IntegerType),
      StructField("parameter_id", StringType),
      StructField("value", LongType),
      StructField("MaxOffset", DoubleType)
    ))
    // write Json of table schema struct
    metabigintgrouped.selectExpr("CAST(adi_flight_record_number AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value").
      write
      .format("kafka")
      .option("topic", "topic_res")
      .option("enable.idempotence", "true")
      .option("transactional.id", "prod-1")
      //.option("auto.commit.enable","true")
      //.option("auto.offset.reset","latest")
      .option("kafka.bootstrap.servers", "hostname:port")
      .save()
    // read Json of table schema struct
    val df = spark
      .read
      .format("kafka")
      .option("enable.auto.commit","false")
      .option("isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "hostname:port")
      .option("subscribe", "topic_res")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", metabigintgroupedSchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.show(false)


  }

}
