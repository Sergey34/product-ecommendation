package seko.spark.com.mlib.ottoc.clientov


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*


fun main(args: Array<String>) {

    System.setProperty("hadoop.home.dir", "/home/sergey/IdeaProjects/product-ecommendation/conf")

    Logger.getLogger("org").level = Level.OFF
    Logger.getLogger("akka").level = Level.OFF


    val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("prediction")
            .orCreate


    val schema = StructType(arrayOf(
            StructField("category_title", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("price", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_1", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_2", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_3", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_4", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_5", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_6", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_7", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_8", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_9", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("characteristic_10", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("flag", `StringType$`.`MODULE$`, true, Metadata.empty())
    ))


    val dataset = SQLContext(spark).read()
            .format(CSV)
            .option("delimiter", ",")
            .option("header", true)
            .schema(schema)
            .load("churn.all/products.csv")

    val split = dataset.randomSplit(doubleArrayOf(0.7, 0.3))
    val train = split[0]
    val test = split[1]

    val labelIndexer = StringIndexer().setInputCol("flag").setOutputCol("label")
    val categoryTitleIndexer = StringIndexer().setInputCol("category_title").setOutputCol("category_title_int").setHandleInvalid("keep")
    val characteristic1Indexer = StringIndexer().setInputCol("characteristic_1").setOutputCol("characteristic_1_int").setHandleInvalid("keep")
    val characteristic2Indexer = StringIndexer().setInputCol("characteristic_2").setOutputCol("characteristic_2_int").setHandleInvalid("keep")
    val characteristic3Indexer = StringIndexer().setInputCol("characteristic_3").setOutputCol("characteristic_3_int").setHandleInvalid("keep")
    val characteristic4Indexer = StringIndexer().setInputCol("characteristic_4").setOutputCol("characteristic_4_int").setHandleInvalid("keep")
    val characteristic5Indexer = StringIndexer().setInputCol("characteristic_5").setOutputCol("characteristic_5_int").setHandleInvalid("keep")
    val characteristic6Indexer = StringIndexer().setInputCol("characteristic_6").setOutputCol("characteristic_6_int").setHandleInvalid("keep")
    val characteristic7Indexer = StringIndexer().setInputCol("characteristic_7").setOutputCol("characteristic_7_int").setHandleInvalid("keep")
    val characteristic8Indexer = StringIndexer().setInputCol("characteristic_8").setOutputCol("characteristic_8_int").setHandleInvalid("keep")
    val characteristic9Indexer = StringIndexer().setInputCol("characteristic_9").setOutputCol("characteristic_9_int").setHandleInvalid("keep")
    val characteristic10Indexer = StringIndexer().setInputCol("characteristic_10").setOutputCol("characteristic_10_int").setHandleInvalid("keep")

    val assembler = VectorAssembler()
            .setInputCols(arrayOf("price",
                    "category_title_int",
                    "characteristic_1_int",
                    "characteristic_2_int",
                    "characteristic_3_int",
                    "characteristic_4_int",
                    "characteristic_5_int",
                    "characteristic_6_int",
                    "characteristic_7_int",
                    "characteristic_8_int",
                    "characteristic_9_int",
                    "characteristic_10_int"))
            .setOutputCol("features")

    val classifier = RandomForestClassifier()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setMaxBins(600)

    val pipeline = Pipeline().setStages(arrayOf(categoryTitleIndexer,
            characteristic1Indexer,
            characteristic2Indexer,
            characteristic3Indexer,
            characteristic4Indexer,
            characteristic5Indexer,
            characteristic6Indexer,
            characteristic7Indexer,
            characteristic8Indexer,
            characteristic9Indexer,
            characteristic10Indexer,
            labelIndexer, assembler, classifier))

    val model = pipeline.fit(train)

    val predictions = model.transform(test)

    println("train")
    train.show()
    println("model")
    model.transform(train).show()
    println("test")
    predictions.show()

}