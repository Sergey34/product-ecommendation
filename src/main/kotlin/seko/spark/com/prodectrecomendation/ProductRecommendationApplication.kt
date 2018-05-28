package seko.spark.com.prodectrecomendation

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
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
//    ProductRecommendation().calculate()
    runApplication<ProductRecommendationApplication>(*args)
}
