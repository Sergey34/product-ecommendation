package seko.spark.com.productrecomendation.kafka


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.Optional
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.Serializable
import scala.Tuple2
import java.util.concurrent.Future
import java.util.regex.Pattern


/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <groupId> is a consumer group name to consume from topics
 * <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 * $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 * consumer-group topic1,topic2
</topics></groupId></brokers></topics></groupId></brokers> */

object JavaDirectKafkaWordCount {
    private val SPACE = Pattern.compile(" ")
    private lateinit var jssc: JavaStreamingContext

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val brokers = "192.168.0.106:9092"
        val groupId = "wc"
        val topics = "wc-topic"

        // Create context with a 2 seconds batch interval
        val sparkConf = SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]")

        val javaSparkContext = JavaSparkContext(sparkConf)

        jssc = JavaStreamingContext(javaSparkContext, Durations.seconds(30))
        jssc.checkpoint("./checkpoint")

        val topicsSet = topics.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toSet()

        val kafkaParams = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )

        // Create direct kafka stream with brokers and topics
        val messages: JavaInputDStream<ConsumerRecord<String, String>> = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams))

        // Get the lines, split them into words, count the words and print
        val lines = messages.map { it.value() }
        val words = lines.flatMap { x -> listOf(*SPACE.split(x)).iterator() }
        val wordsDstream = words
                .mapToPair { s -> Tuple2<String, Int>(s, 1) }
                .reduceByKey { i1, i2 -> i1 + i2 }

        val mappingFunc: Function3<String, Optional<Int>, State<Int>, Tuple2<String, Int>> =
                { word, one, state ->
                    val sum = one.orElse(0) + if (state.exists()) state.get() else 0
                    val output = Tuple2<String, Int>(word, sum)
                    state.update(sum)
                    output
                }

        val stateDstream = wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(getInitialRDD()))

        stateDstream.print()


        val prop = mutableMapOf<String, String>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name)


        val kafkaSink = javaSparkContext.broadcast(KafkaSink(prop, createProducer = { parms -> KafkaProducer(parms) }))

        stateDstream.foreachRDD { rdd ->
            rdd.foreachPartition { partitionOfRecords ->
                partitionOfRecords.forEach { message ->
                    kafkaSink.value.send("wc-result", message)
                }
            }
        }

        // Start the computation
        jssc.start()
        jssc.awaitTermination()
    }

    private fun getInitialRDD(): JavaPairRDD<String, Int> {
        return jssc.sparkContext().parallelizePairs(listOf())
    }
}

class KafkaSink(var parms: Map<String, String>, var createProducer: (parms: Map<String, String>) -> KafkaProducer<String, String>) : Serializable {

    private var producer: KafkaProducer<String, String>? = null

    fun send(topic: String, value: Tuple2<String, Int>): Future<RecordMetadata> {
        if (producer == null) {// потому что надо как-то сериализовать несириализуемое....
            producer = createProducer(parms)
        }
        return producer?.send(ProducerRecord(topic, value.toString()))!!
    }

}
