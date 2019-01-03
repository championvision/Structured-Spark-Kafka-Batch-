package comgeAviation.scala

//Imported the required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object task {
  def main(args: Array[String]): Unit = {

    // Created SparkSession
    val spark:SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate();
    import spark.implicits._
    var sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    // Passed sub_part1 parameter into the query
    //val week = "('2018-08')"
    //val query  = s"select * from time_series.raw_bigint where sub_part1 in $week limit 3"
    //var raw_bigint_df = spark.sql(query)
    var raw_bigint_df = spark.sql("select * from default.raw_bigint_in_time_series_20rows");
    // bigint dataframe schema
    val bigintSchema = StructType(Array(
      StructField("unique_id", LongType),
      StructField("offset", DoubleType),
      StructField("value", LongType),
      StructField("ems_system_id", IntegerType),
      StructField("file_type", StringType),
      StructField("sub_part1", StringType),
      StructField("parameter_id", StringType),
      StructField("sub_part2", StringType)
    ))
    // write data frame to the topic in json format of schema structure.
    raw_bigint_df.selectExpr("CAST(unique_id AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value").
      write
      .format("kafka")
      .option("topic", "topic_bigint")
      .option("enable.idempotence", "true")
      .option("transactional.id", "prod-1")
      //.option("auto.commit.enable","true")
      //.option("auto.offset.reset","latest")
      .option("kafka.bootstrap.servers", "hostname:port")
      .save()
      // read data frame from the topic in json format of schema structure.
    val df = spark
      .read
      .format("kafka")
      .option("enable.auto.commit","false")
      .option("isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "hostname:port")
      .option("subscribe", "topic_bigint")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", bigintSchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.show(false)


    var raw_metadata_df = spark.sql("select * from default.meta_try");
    // metadata dataframe schema
    val metadataSchema = StructType(Array(
      StructField("unique_id", LongType),
      StructField("scheduled_ingestion_timestamp",LongType),
      StructField("ingestion_timestamp", TimestampType),
      StructField("flight_date_exact", StringType),
      StructField("adi_flight_record_number", IntegerType),
      StructField("parameter_table", StringType),
      StructField("data_type", StringType),
      StructField("raw_metadata_string", StringType),
      StructField("customer_code", StringType),
      StructField("tail_number", StringType),
      StructField("flight_number", StringType),
      StructField("decode_generation_time", StringType),
      StructField("airport_depart", StringType),
      StructField("airport_arrival", StringType),
      StructField("export_config_version", StringType),
      StructField("parameter_id", StringType),
      StructField("sub_part2", StringType),
      StructField("ems_system_id", IntegerType),
      StructField("file_type", StringType),
      StructField("sub_part1", StringType)
    ))

    // write data frame to the topic in json format of schema structure.
    raw_metadata_df.selectExpr("CAST(unique_id AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value").
      write
      .format("kafka")
      .option("topic", "topic_meta")
      .option("enable.idempotence", "true")
      .option("transactional.id", "prod-metadata")
      ///.option("auto.commit.enable","true")
      ///.option("auto.offset.reset","latest")
      .option("kafka.bootstrap.servers", "hostname:port")
      .save()

    //raw_metadata_df.write.saveAsTable("default.meta_try")

    // read data frame from the topic in json format of schema structure.
    val dff = spark
      .read
      .format("kafka")
      .option("enable.auto.commit","false")
      .option("isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "hostname:port")
      .option("subscribe", "topic_meta")
      .load()


    val dff1 = dff.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", metadataSchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    //df1.write.saveAsTable("default.raw_bigint_in_time_series_20Rows");
    dff1.show(false)



  }
}
