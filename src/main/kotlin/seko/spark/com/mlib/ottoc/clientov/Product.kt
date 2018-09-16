package seko.spark.com.mlib.ottoc.clientov

import java.io.Serializable
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.*


data class Product(var id: String = UUID.randomUUID().toString(),
                   var title: String = UUID.randomUUID().toString(),
                   var description: String = UUID.randomUUID().toString(),
                   var account: String = UUID.randomUUID().toString(),
                   var price: BigDecimal = BigDecimal.TEN,
                   var category: Category = Category("title", "descr"),
                   var characteristic: List<Characteristic> = category.characteristics,
                   var tags: List<String> = listOf("tag1", "tag2"),
                   var productInfo: String = "",
                   var enabled: Boolean = false,
                   var date: String = LocalDateTime.now().toString()) : Serializable


data class Category(var title: String,
                    var description: String = "",
                    var characteristics: List<Characteristic> = listOf()) : Serializable {
}

data class Characteristic(var name: String, var value: String? = null) : Serializable