package seko.spark.com.productrecomendation.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import scala.Serializable
import scala.Tuple2
import java.util.concurrent.Future

class KafkaSink(var parms: Map<String, String>, var createProducer: (parms: Map<String, String>) -> KafkaProducer<String, String>) : Serializable {

    private var producer: KafkaProducer<String, String>? = null

    fun send(topic: String, value: Tuple2<Any, List<Tuple2<Any, Int>>>): Future<RecordMetadata> {
        if (producer == null) {// потому что надо как-то сериализовать несириализуемое....
            producer = createProducer(parms)
        }
        return producer?.send(ProducerRecord(topic, value.toString()))!!
    }

}