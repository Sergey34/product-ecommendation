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
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.api.java.Optional
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.Tuple2
import java.util.*
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
        jssc = JavaStreamingContext(sparkConf, Durations.seconds(30))
        jssc.checkpoint("./checkpoint")

        val topicsSet = HashSet(Arrays.asList(*topics.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()))
        val kafkaParams = HashMap<String, Any>()
        kafkaParams[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
        kafkaParams[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        kafkaParams[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        kafkaParams[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        // Create direct kafka stream with brokers and topics
        val messages: JavaInputDStream<ConsumerRecord<String, String>> = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams))

        // Get the lines, split them into words, count the words and print
        val lines = messages.map { it.value() }
        val words = lines.flatMap { x -> Arrays.asList(*SPACE.split(x)).iterator() }
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

        // Start the computation
        jssc.start()
        jssc.awaitTermination()
    }

    private fun getInitialRDD(): JavaPairRDD<String, Int> {
        val tuples:List<Tuple2<String,Int>> = listOf()
        return jssc.sparkContext().parallelizePairs(tuples)
    }
}

