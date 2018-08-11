package seko.spark.com.productrecomendation.kafka

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import javax.annotation.PostConstruct

@SpringBootApplication
class ProductRecommendationApplication {
    @Autowired
    lateinit var productRecommendation: ProductRecommendation

    @PostConstruct
    fun init() {
        productRecommendation.calculate()
    }
}

fun main(args: Array<String>) {
//    val productRecommendation = ProductRecommendation()
//    productRecommendation.initKafkaParams()
//    productRecommendation.calculate()
    runApplication<ProductRecommendationApplication>(*args)
}
