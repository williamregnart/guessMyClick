import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler

object Main extends App {
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

	val spark = SparkSession
		.builder()
		.master("local")
		.appName("GuessMyClick")
		.config("", "")
		.getOrCreate()

	val data = spark.read
		.option("header", "true")
		.option("delimiter", ",")
		.option("inferSchema", "true")
		.json("data-students.json")

	import spark.implicits._

	println("---data columns---")
	data.columns.foreach(println)

	def rlikeFor(str: String): String = {
		s"$str-|$str,|$str$$"
	}

	val refined_data = data
		.withColumn("is_app",
			when(data.col("appOrSite").equalTo("site"), 0)
				.otherwise(
					when(data.col("appOrSite").equalTo("app"), 1)
						.otherwise(-1)
				)
		)
		.drop("appOrSite")
		.drop("bidFloor")
		.drop("city")
		.drop("exchange")
		.drop("impid")
		.withColumn("is_clicked",
			when(data.col("label").equalTo("false"), 0)
				.otherwise(
					when(data.col("label").equalTo("true"), 1)
						.otherwise(-1)
				)
		)
		.drop("label")
		.drop("network")
		.withColumn("is_ios",
			when(data.col("os").equalTo("iOS"), 1)
				.otherwise(
					when(data.col("os").equalTo("Android"), 0)
						.otherwise(-1)
				)
		)
		.drop("os")
		.drop("publisher")
		.drop("size")
		.drop("timestamp")
		.withColumn("refined_type",
			when(data.col("type").isNull, -1)
				.otherwise(data.col("type").cast(sql.types.IntegerType))
		)
		.drop("type")
		.drop("user")
		.withColumn("has_iab1",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB1")))
				.otherwise(false)
		)
		.withColumn("has_iab2",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB2")))
				.otherwise(false)
		)
		.withColumn("has_iab3",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB3")))
				.otherwise(false)
		)
		.withColumn("has_iab4",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB4")))
				.otherwise(false)
		)
		.withColumn("has_iab5",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB5")))
				.otherwise(false)
		)
		.withColumn("has_iab6",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB6")))
				.otherwise(false)
		)
		.withColumn("has_iab7",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB7")))
				.otherwise(false)
		)
		.withColumn("has_iab8",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB8")))
				.otherwise(false)
		)
		.withColumn("has_iab9",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB9")))
				.otherwise(false)
		)
		.withColumn("has_iab10",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB10")))
				.otherwise(false)
		)
		.withColumn("has_iab11",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB11")))
				.otherwise(false)
		)
		.withColumn("has_iab12",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB12")))
				.otherwise(false)
		)
		.withColumn("has_iab13",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB13")))
				.otherwise(false)
		)
		.withColumn("has_iab14",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB14")))
				.otherwise(false)
		)
		.withColumn("has_iab15",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB15")))
				.otherwise(false)
		)
		.withColumn("has_iab16",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB16")))
				.otherwise(false)
		)
		.withColumn("has_iab17",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB17")))
				.otherwise(false)
		)
		.withColumn("has_iab18",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB18")))
				.otherwise(false)
		)
		.withColumn("has_iab19",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB19")))
				.otherwise(false)
		)
		.withColumn("has_iab20",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB20")))
				.otherwise(false)
		)
		.withColumn("has_iab21",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB21")))
				.otherwise(false)
		)
		.withColumn("has_iab22",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB22")))
				.otherwise(false)
		)
		.withColumn("has_iab23",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB23")))
				.otherwise(false)
		)
		.withColumn("has_iab24",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB24")))
				.otherwise(false)
		)
		.withColumn("has_iab25",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB25")))
				.otherwise(false)
		)
		.withColumn("has_iab26",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB26")))
				.otherwise(false)
		)
		.drop("interests")
		.drop("media")

	val re_refined_data = refined_data.withColumnRenamed("is_clicked", "label")


	println("---refined data columns---")
	re_refined_data.columns.foreach(println)

	println("---refined data preview---")
	re_refined_data.show(20, truncate = false)

	//	println("---interests values---")
	//	val pattern = """IAB\d{1,2}""".r
	//	refined_data.select("interests").as[String].collect().filter(l => l != null).flatMap(_.split(",")).filter(_.startsWith("IAB")).map(e => pattern.findAllIn(e).mkString).distinct.sorted.foreach(println)

	val assembler = new VectorAssembler()
		.setInputCols(re_refined_data.columns.filter(_ != "label"))
		.setOutputCol("features")
  	.setHandleInvalid("skip")

	val features = assembler.transform(re_refined_data)
	features.show(5)

	val splits = features.randomSplit(Array(0.8, 0.2))
	val train = splits(0)
	val test = splits(1)

	val layers = Array[Int](29, 10, 10, 2)

	val trainer = new MultilayerPerceptronClassifier()
		.setLayers(layers)
		.setMaxIter(100)

	val model = trainer.fit(train)

	val result = model.transform(test)
	val predictionAndLabels = result.select("prediction", "label")
	val evaluator = new MulticlassClassificationEvaluator()
		.setMetricName("accuracy")

	println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
}