package seko.spark.com.productrecomendation.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import scala.Serializable
import scala.Tuple2
import java.util.*
import javax.annotation.PostConstruct

@Component
class ProductRecommendation : Serializable {
    @Value("\${kafka.brokers}")
    lateinit var brokers: String //= "192.168.0.106:9092"
    @Value("\${kafka.groupId}")
    lateinit var groupId: String // = "wc"
    @Value("\${kafka.outputTopic}")
    lateinit var outputTopic: String // = "wc"
    @Value("\${kafka.inputTopics}")
    lateinit var inputTopics: Array<String> //= "wc-topic"

    @Value("\${spark.master}")
    lateinit var master: String //"local[*]"
    @Value("\${spark.appName}")
    lateinit var appName: String //"ProductRecommendation"
    @Value("\${spark.checkpointDirectory}")
    lateinit var checkpointDirectory: String //"./checkpoint"
    @Value("\${spark.durations}")
    var durations: Long = 5 //30l

    lateinit var kafkaConsumerProperties: Map<String, java.io.Serializable>
    lateinit var kafkaProducerProperties: MutableMap<String, String>

    @PostConstruct
    fun initKafkaParams() {

        kafkaConsumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )

        kafkaProducerProperties = mutableMapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name)
    }

    val mappingFunc: Function3<Any, Optional<List<Tuple2<Any, Int>>>, State<List<Tuple2<Any, Int>>>, Tuple2<Any, List<Tuple2<Any, Int>>>> =
            { productId, currentValue, state ->
                val stateValue = if (state.exists()) state.get() else listOf()//[(12,3),(11,3),(15,2),(1,4)]

                val map = mutableMapOf<Any, Int>()
                (currentValue.orElse(listOf()) + stateValue).forEach {
                    map[it._1] = map.getOrDefault(it._1, 0) + it._2
                }
                val sum = map.map { Tuple2<Any, Int>(it.key, it.value) }

                val output = Tuple2<Any, List<Tuple2<Any, Int>>>(productId, sum)
                state.update(sum)
                output
            }

    fun calculate() {
        val sparkConf: SparkConf = SparkConf().setAppName(appName).setMaster(master)
        val jssc: JavaStreamingContext
        val javaSparkContext = JavaSparkContext(sparkConf)
        // Create context with a 2 seconds batch interval
        jssc = JavaStreamingContext(javaSparkContext, Durations.seconds(durations))
        jssc.checkpoint(checkpointDirectory)

        // Create direct kafka stream with brokers and inputTopics
        val stream: JavaInputDStream<ConsumerRecord<String, String>> = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(inputTopics.toList(), kafkaConsumerProperties))

        val dstream = stream
                .mapToPair { parseEvent(it.value()) }
                .reduceByKey { list1, list2 -> list1.union(list2).distinct().sorted() }
                .filter { it._2().size > 1 }
                .flatMap { createPairs(it) }
                .mapToPair { Tuple2(it, 1) }
                .reduceByKey { a, b -> Integer.sum(a, b) }
                .flatMapToPair { listOf(Tuple2(it._1._1, listOf(Tuple2(it._1._2, it._2))), Tuple2(it._1._2, listOf(Tuple2(it._1._1, it._2)))).iterator() }
                .reduceByKey { list1, list2 -> list1.union(list2).toList() }
                .mapWithState(StateSpec.function(mappingFunc).initialState(getInitialRDD(jssc)))

        dstream.print()


        val kafkaSink = javaSparkContext.broadcast(KafkaSink(kafkaProducerProperties, createProducer = { config -> KafkaProducer(config) }))

        dstream.foreachRDD { rdd ->
            rdd.foreachPartition { partitionOfRecords ->
                partitionOfRecords.forEach { message ->
                    kafkaSink.value.send(outputTopic, message)
                }
            }
        }

        // Start the computation
        jssc.start()
        jssc.awaitTermination()
    }

    private fun parseEvent(it: String): Tuple2<String, List<String>> {
        val split = it.split(" ")
        return Tuple2(split[0], listOf(split[1]))
    }

    private fun createPairs(it: Tuple2<String, List<String>>?): MutableIterator<Tuple2<*, *>> {
        val result = ArrayList<Tuple2<*, *>>()
        for (i in 0 until it!!._2().size - 1) {
            val key = it._2()[i]
            for (j in i + 1 until it._2().size) {
                result.add(Tuple2(key, it._2()[j]))
            }
        }
        return result.iterator()
    }

    private fun getInitialRDD(jssc: JavaStreamingContext): JavaPairRDD<Any, List<Tuple2<Any, Int>>> {
        return jssc.sparkContext().parallelizePairs(listOf())
    }
}
