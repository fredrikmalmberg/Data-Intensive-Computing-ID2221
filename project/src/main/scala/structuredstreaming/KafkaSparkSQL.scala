package structuredstreaming

import java.sql.Timestamp
import java.util.Calendar

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.google.gson.{Gson, JsonObject}
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import org.apache.spark.sql.streaming.OutputMode

object KafkaSparkSQL {
    def main(args: Array[String]) : Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("tweets").set("spark.port.maxRetries", "100")
        val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
        import spark.implicits._

        val cluster = Cluster.builder.addContactPoint("127.0.0.1").withoutJMXReporting.build
        val session = cluster.connect
        session.execute("CREATE KEYSPACE IF NOT EXISTS tweets_space WITH REPLICATION = " +
            "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS tweets_space.tweets (id text PRIMARY KEY, timestamp timestamp);")
        session.execute("CREATE TABLE IF NOT EXISTS tweets_space.batches (timestamp timestamp PRIMARY KEY, ids text); ")

        val init_df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "tweets")
            .load

        val df = init_df.selectExpr("CAST(value AS STRING)", "timestamp")

        val parsed_ids = df.map(x => {
            val jobj = new Gson().fromJson(x.getString(0), classOf[JsonObject])
            val id = jobj.get("data").getAsJsonObject.get("id").getAsString
            (id, x.getTimestamp(1))
        })

        val timeWindow = 20
        val batches = parsed_ids.map(x => {
            val seconds = x._2.getTime() / 1000
            val timeframe = seconds - (seconds % timeWindow)
            (new Timestamp(timeframe * 1000) , x._1)
        })
            .groupBy("_1")
            .agg(concat_ws(",", collect_list("_2")) as "ids")

        val cass_query = parsed_ids
            .withColumnRenamed("_1", "id")
            .withColumnRenamed("_2", "timestamp")
            .writeStream
            .outputMode(OutputMode.Append)
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "checkpoint")
            .option("keyspace", "tweets_space")
            .option("table", "tweets")
            .start

        val cass_query_2 = batches
            .withColumnRenamed("_1", "timestamp")
            .withColumnRenamed("_2", "ids")
            .writeStream
            .outputMode("update")
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "checkpoint")
            .option("keyspace", "tweets_space")
            .option("table", "batches")
            .start

        val kafka_query = batches.writeStream
            .format("kafka")
            .outputMode(OutputMode.Update)
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "lookup")
            .start

        spark.streams.awaitAnyTermination()
        spark.close
        session.close
    }
}
