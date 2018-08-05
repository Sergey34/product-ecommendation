package seko.spark.com.productrecomendation.rdd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.springframework.stereotype.Component
import scala.Serializable
import scala.Tuple2
import java.util.*

@Component
class ProductRecommendation : Serializable {
    fun calculate() {
        val sparkConf = SparkConf()
        sparkConf.setAppName("ProductRecommendation")
        sparkConf.setMaster("local[*]")
        val sc = JavaSparkContext(sparkConf)

        val rdd = sc.textFile("data/tt.txt")
        rdd.persist(MEMORY_AND_DISK())


        val collect = rdd
                .mapToPair { parseEvent(it) }
                .reduceByKey { list1, list2 -> list1.union(list2).distinct().sorted() }
                .filter { it._2().size > 1 }
                .flatMap { createPairs(it) }
                .mapToPair { Tuple2(it, 1) }
                .reduceByKey { a, b -> Integer.sum(a, b) }
                .filter { it._2() > 1 }
                .flatMapToPair { listOf(Tuple2(it._1._1, listOf(Tuple2(it._1._2, it._2))), Tuple2(it._1._2, listOf(Tuple2(it._1._1, it._2)))).iterator() }
                .reduceByKey { list1, list2 -> list1.union(list2).toList() }
                .collect()


        println(mapOf(*collect.map { Pair(it._1, it._2) }.toTypedArray())) //{8=[(7,2), (6,2)], 4=[(2,3), (5,3)], 6=[(7,2), (8,2)], 2=[(4,3)], 7=[(6,2), (8,2)], 5=[(4,3)]}
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
}