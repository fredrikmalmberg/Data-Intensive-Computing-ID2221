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
import scala.collection.JavaConversions.iterableAsScalaIterable


object KafkaSpark {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("tweets").set("spark.port.maxRetries", "100")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("checkpoint")

        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS tweets_space WITH REPLICATION = " +
                        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("CREATE TABLE IF NOT EXISTS tweets_space.tweets (id text PRIMARY KEY," +
            "input list<frozen<list<int>>>, target list<int>, content text, rules list<text>, timestamp int);")
        session.execute("CREATE TABLE IF NOT EXISTS tweets_space.batches (timestamp int PRIMARY KEY, ids text);")

        val kafkaConf = Map(
            "metadata.broker.list" -> "localhost:9092",
            "zookeeper.connect" -> "localhost:2181",
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000"
        )

        val ids = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set("tweets"))
        val lookups = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set("lookup"))
        val finalLookups = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set("final_lookup"))

        val lookupMapFunc = (x : (Array[Byte], String)) => {
            val jobj = new Gson().fromJson(x._2, classOf[JsonObject])
            val data = jobj.getAsJsonArray("data")
            val res = (for (entry <- data) yield {
                val entryObj = entry.getAsJsonObject
                val id = entryObj.get("id").getAsString
                val metricsObj = entryObj.getAsJsonObject("public_metrics")
                val likeCount = metricsObj.get("like_count").getAsInt
                val quoteCount = metricsObj.get("quote_count").getAsInt
                val replyCount = metricsObj.get("reply_count").getAsInt
                val retweetCount = metricsObj.get("retweet_count").getAsInt
                (id, List(likeCount, quoteCount, replyCount, retweetCount))
            }).toList
            if (jobj.has("errors")) {
                val errors = jobj.getAsJsonArray("errors")
                val ids = for (entry <- errors) {
                    val entryObj = entry.getAsJsonObject
                    val title = entryObj.get("title").getAsString
                    if (title == "Not Found Error") {
                        (entryObj.get("value").getAsString, List()) :: res
                    }
                }
            }
            res
        }

        val lookupPairs = lookups.flatMap(lookupMapFunc)
        val finalLookupPairs = finalLookups.flatMap(lookupMapFunc)

        val lookupUpdateFunc = (key: String, value: Option[List[Int]], state:State[List[List[Int]]]) => {
            val newVal = value.getOrElse(List())
            val list = state.getOption.getOrElse(List())
            val update = newVal :: list
            state.update(update)
            (key, update)
        }

        val lookupResults = lookupPairs.mapWithState(StateSpec.function(lookupUpdateFunc))

        lookupResults.saveToCassandra("tweets_space", "tweets", SomeColumns("id", "input"))
        finalLookupPairs.saveToCassandra("tweets_space", "tweets", SomeColumns("id", "target"))

        val tweetInfo = ids.map(x => {
            val jobj = new Gson().fromJson(x._2, classOf[JsonObject])
            val data = jobj.getAsJsonObject("data")
            val id = data.get("id").getAsString
            val content = data.get("text").getAsString
            val timestamp = jobj.get("timestamp").getAsInt
            val tags = for (rule <- jobj.getAsJsonArray("matching_rules")) yield {
                rule.getAsJsonObject.get("tag").getAsString
            }
            (id, timestamp, content, tags)
        })

        tweetInfo.saveToCassandra("tweets_space", "tweets", SomeColumns("id", "timestamp", "content", "rules"))

        val idPairs = tweetInfo.map(x => (x._1, x._2))

        val timeWindow = 20
        val windowedIdPairs = idPairs.map(x => (x._2 - (x._2 % timeWindow), x._1))

        val batchUpdateFunc = (key: Int, value: Option[String], state: State[(String, Int)]) => {
            val newVal = value.getOrElse("")
            val (oldVal, count) = state.getOption.getOrElse("", 0)
            val concat = if (count == 0) {
                val concat = newVal
                state.update(concat, 1)
                concat
            } else if (count < 100) {
                val concat = oldVal + "," + newVal
                state.update(concat, count + 1)
                concat
            } else oldVal
            (key, concat)
        }

        val batches = windowedIdPairs.mapWithState(StateSpec.function(batchUpdateFunc))

        batches.saveToCassandra("tweets_space", "batches")

        ssc.start()
        ssc.awaitTermination()
        session.close()
    }
}
