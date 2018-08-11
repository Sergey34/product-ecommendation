package seko.spark.com.productrecomendation.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.Optional
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.State
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Service
import scala.Tuple2
import seko.spark.com.productrecomendation.kafka.configs.KafkaProperties
import seko.spark.com.productrecomendation.kafka.configs.SparkProperties
import java.io.Serializable
import javax.annotation.PostConstruct

@SpringBootApplication
class ProductRecommendationApplication

fun main(args: Array<String>) {
    runApplication<ProductRecommendationApplication>(*args)
}

@Service
class ProductRecommendationService {
    @Autowired
    lateinit var jssc: JavaStreamingContext
    @Autowired
    lateinit var kafProperties: KafkaProperties
    @Autowired
    lateinit var productRecommendation: ProductRecommendation

    @PostConstruct
    fun init() {
        val preferConsistent = LocationStrategies.PreferConsistent()
        val subscribe = ConsumerStrategies.Subscribe<String, String>(kafProperties.inputTopics.toList(), kafProperties.kafkaConsumerProperties)
        val stream = KafkaUtils.createDirectStream(jssc, preferConsistent, subscribe)
        productRecommendation.calculate(stream, jssc)

    }
}

@ComponentScan(basePackages = ["seko.spark.com.productrecomendation.kafka"])
@Configuration
class BeanConfiguration {

    @Bean
    fun sparkConf(sparkProperties: SparkProperties): SparkConf {
        return SparkConf().setAppName(sparkProperties.appName).setMaster(sparkProperties.master)
    }

    @Bean
    fun javaStreamingContext(sparkProperties: SparkProperties): JavaStreamingContext {
        val jssc = JavaStreamingContext(javaSparkContext(sparkProperties), Durations.seconds(sparkProperties.durations))
        jssc.checkpoint(sparkProperties.checkpointDirectory)
        return jssc
    }

    @Bean
    fun javaSparkContext(sparkProperties: SparkProperties): JavaSparkContext {
        return JavaSparkContext(sparkConf(sparkProperties))
    }

    @Bean
    fun kafkaSink(kafkaProperties: KafkaProperties): KafkaSink {
        return KafkaSink(kafkaProperties, createProducer = { config -> KafkaProducer(config) })
    }

    @Bean
    fun mappingFunc(): (Any, Optional<List<Tuple2<Any, Int>>>, State<List<Tuple2<Any, Int>>>) -> Tuple2<Any, List<Tuple2<Any, Int>>> {
        return { productId, currentValue, state ->
            val stateValue = if (state.exists()) state.get() else listOf()//[(12,3),(11,3),(15,2),(1,4)]

            val map = mutableMapOf<Any, Int>()
            (currentValue.orElse(listOf()) + stateValue).forEach {
                map[it._1] = map.getOrDefault(it._1, 0) + it._2
            }
            val sum = map.map { Tuple2(it.key, it.value) }

            val output = Tuple2(productId, sum)
            state.update(sum)
            output
        }
    }

    @Bean
    fun productRecommendation(kafkaProperties: KafkaProperties, javaSparkContext: JavaSparkContext): ProductRecommendation {
        val kafkaSink = javaSparkContext.broadcast(kafkaSink(kafkaProperties))
        val mappingFunc = javaSparkContext.broadcast(mappingFunc())
        return ProductRecommendation(kafkaSink, mappingFunc)
    }

    @Bean
    @Qualifier("kafkaConsumerProperties")
    fun kafkaConsumerProperties(@Value("\${kafka.brokers}") brokers: String,
                                @Value("\${kafka.groupId}") groupId: String): Map<String, Serializable> {
        return mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java)
    }

    @Bean
    @Qualifier("kafkaProducerProperties")
    fun kafkaProducerProperties(@Value("\${kafka.brokers}") brokers: String): MutableMap<String, String> {
        return mutableMapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name)
    }
}