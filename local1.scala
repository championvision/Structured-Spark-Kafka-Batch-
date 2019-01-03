package comgeAviation.scala

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object local {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("KafkaSparkDemo")
      .getOrCreate()
    import spark.implicits._
    var sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

     val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )

    val schema = StructType(Array(
      StructField("unique_id", IntegerType),
      StructField("word", StringType)
    ))

    val raw_bigint_df = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(schema)
    )
    var raw_bigint_df2 = raw_bigint_df.filter($"unique_id" === 8)
    raw_bigint_df2.selectExpr("CAST(unique_id AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value").
      write
      .format("kafka")
      .option("topic", "topic1")
      .option("enable.idempotence", "true")
      .option("transactional.id", "prod-1")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .save()

    val df = spark
      .read
      .format("kafka")
      .option("enable.auto.commit","false")
      .option("isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic1")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", schema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.show(false)

    var raw_bigint_df3 = raw_bigint_df.filter($"unique_id" === 8)
    raw_bigint_df3.selectExpr("CAST(unique_id AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value").
      write
      .format("kafka")
      .option("topic", "topic2")
      .option("enable.idempotence", "true")
      .option("transactional.id", "prod-1")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .save()

    val df2 = spark
      .read
      .format("kafka")
      .option("enable.auto.commit","false")
      .option("isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic2")
      .load()

    val df3 = df2.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", schema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df3.show(false)




  }

}
