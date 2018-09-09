package seko.spark.com.mlib.ottoc.clientov


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*

const val CSV = "csv"

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
            StructField("state", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("account_length", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("area_code", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("phone_number", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("intl_plan", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("voice_mail_plan", `StringType$`.`MODULE$`, true, Metadata.empty()),
            StructField("number_vmail_messages", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_day_minutes", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_day_calls", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_day_charge", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_eve_minutes", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_eve_calls", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_eve_charge", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_night_minutes", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_night_calls", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_night_charge", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_intl_minutes", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_intl_calls", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("total_intl_charge", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("number_customer_service_calls", `DoubleType$`.`MODULE$`, true, Metadata.empty()),
            StructField("churned", `StringType$`.`MODULE$`, true, Metadata.empty())))


    val dataset = SQLContext(spark).read()
            .format(CSV)
            .option("delimiter", ",")
            .option("header", true)
            .schema(schema)
            .load("churn.all/tmp.csv")

    dataset.show()

    val split = dataset.randomSplit(doubleArrayOf(0.7, 0.3))
    val train = split[0]
    val test = split[1]

    val labelIndexer = StringIndexer().setInputCol("churned").setOutputCol("label")
    val stringIndexer = StringIndexer().setInputCol("intl_plan").setOutputCol("intl_plan_indexed")
    val reducedNumericCols = arrayOf("account_length", "number_vmail_messages", "total_day_calls",
            "total_day_charge", "total_eve_calls", "total_eve_charge",
            "total_night_calls", "total_intl_calls", "total_intl_charge")


    val assembler = VectorAssembler()
            .setInputCols(arrayOf("intl_plan_indexed") + reducedNumericCols)
            .setOutputCol("features")

    val classifier = RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")

    val pipeline = Pipeline().setStages(arrayOf(stringIndexer, labelIndexer, assembler, classifier))

    val model = pipeline.fit(train)

//    model.write().overwrite().save("churn.all/saved")
//    PipelineModel.read().load("churn.all/saved").transform(test).show()

    test.show()

    val predictions = model.transform(test)

    predictions.show()

    val evaluator = BinaryClassificationEvaluator()

    val auroc = evaluator.evaluate(predictions)
    println(auroc)
//    val auroc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})


}