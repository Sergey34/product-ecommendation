package seko.spark.com.dataset

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

class T {

}

fun main(args: Array<String>) {

    val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("JavaStructuredKafkaWordCount")
            .orCreate

    spark.sparkContext().setLogLevel("ERROR")
    spark.sparkContext().setCheckpointDir("checkpoint2")

    // Create DataSet representing the stream of input lines from kafka
    val df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "192.168.0.106:9092")
            .option("subscribe", "wc-topic")
            .load()
    val lines = df
            .selectExpr("CAST(value AS STRING)")
            .`as`(Encoders.STRING())

    // Generate running word count
    val wordCounts = lines
            .flatMap({ x -> x.split(" ").iterator() }, Encoders.STRING())
            .groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream()
            .outputMode("complete")
            .format("console")
            .trigger(Trigger.ProcessingTime(3000))
            .start()

    query.awaitTermination()
}