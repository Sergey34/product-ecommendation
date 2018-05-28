package seko.spark.com.prodectrecomendation

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@SpringBootApplication
@ComponentScan
@Configuration
class ProductRecommendationApplication

fun main(args: Array<String>) {
    System.setProperty("spring.profiles.active", "Dev")
//    val context = AnnotationConfigApplicationContext(ProductRecommendationApplication::class.java)
//    val productRecommendation = context.getBean(ProductRecommendation::class.java)
    ProductRecommendation().calculate()
}
