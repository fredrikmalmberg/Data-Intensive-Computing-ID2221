package sparkstreaming

import java.util.{Properties, Timer, TimerTask}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.SparkSession
import com.google.gson.{Gson, JsonObject}


object KafkaSpark {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("tweets").set("spark.port.maxRetries", "100")
        //val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //import spark.implicits._
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("checkpoint")

        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS tweets_space WITH REPLICATION = " +
                        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS tweets_space.tweets (id text PRIMARY KEY, timestamp int);")

        val kafkaConf = Map(
            "metadata.broker.list" -> "localhost:9092",
            "zookeeper.connect" -> "localhost:2181",
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000"
        )

        val topicSet = Set("tweets")
        val messages = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, topicSet)

        val pairs = messages.map(x => {
            val jobj = new Gson().fromJson(x._2, classOf[JsonObject])
            val id = jobj.get("data").getAsJsonObject.get("id").getAsString
            val timestamp = jobj.get("timestamp").getAsInt
            (id, timestamp)
        })

        val timeWindow = 20
        val batches = pairs.map(x => (x._2 - (x._2 % timeWindow), x._1))
                           .groupByKey()
                           .map(x => (x._1, x._2.toList.mkString(",")))
        val filteredBatches = batches.filter(x => {
            val time = System.currentTimeMillis() / 1000
            val age = time - x._1
            age >= 50 && age < 310 && (((age - 50) % 60) < 20)
        })

        filteredBatches.foreachRDD( rdd => {
            val spark = SparkSession.builder.config(conf).getOrCreate
            import spark.implicits._
            if (!rdd.isEmpty()) {
                val df = rdd.toDF("timestamp", "ids")
                val ds = df
                    .selectExpr("ids")
                    .write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "lookup")
                    .save()
            }
        })
        pairs.saveToCassandra("tweets_space", "tweets")

        ssc.start()
        ssc.awaitTermination()
        session.close()
    }
}
