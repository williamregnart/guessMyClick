import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, DecisionTreeClassifier, LogisticRegression, MultilayerPerceptronClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

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

	//	println("---data columns---")
	//	data.columns.foreach(println)

	def rlikeFor(str: String): String = {
		s"$str-|$str,|$str$$"
	}

	val refined_data = data
		.withColumn("label", data.col("label").cast("integer"))
		.withColumn("is_app",
			when(data.col("appOrSite").equalTo("site"), 0)
				.otherwise(
					when(data.col("appOrSite").equalTo("app"), 1)
						.otherwise(-1)
				)
		)
		.drop("appOrSite")
		.drop("bidFloor")
		.drop("exchange")
		.drop("impid")
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
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB1")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab2",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB2")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab3",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB3")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab4",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB4")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab5",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB5")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab6",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB6")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab7",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB7")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab8",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB8")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab9",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB9")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab10",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB10")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab11",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB11")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab12",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB12")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab13",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB13")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab14",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB14")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab15",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB15")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab16",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB16")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab17",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB17")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab18",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB18")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab19",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB19")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab20",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB20")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab21",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB21")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab22",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB22")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab23",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB23")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab24",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB24")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab25",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB25")).cast("integer"))
				.otherwise(0)
		)
		.withColumn("has_iab26",
			when(data.col("interests").isNotNull, col("interests").rlike(rlikeFor("IAB26")).cast("integer"))
				.otherwise(0)
		)
		.drop("interests")
		.drop("media")
  	.drop("city")

	val city_indexer = new StringIndexer()
		.setInputCol("city")
		.setOutputCol("indexed_city")
		.setHandleInvalid("keep")

//	val refined_data_with_city = city_indexer
//		.fit(refined_data)
//		.transform(refined_data)
//		.drop("city")

	val data_to_use = refined_data

	val assembler = new VectorAssembler()
		.setInputCols(data_to_use.columns.filter(_ != "label"))
		.setOutputCol("features")
		.setHandleInvalid("keep")

	val refined_data_with_features = assembler.transform(data_to_use)

	val featuresAndLabel_data = refined_data_with_features.select("features", "label")

	println("---data preview---")
	data.show(5, truncate = true)

	println("---refined data preview---")
	refined_data.show(5, truncate = true)


	println("---features data preview---")
	refined_data_with_features.show(5, truncate = false)

	println("---featuresAndLabel data preview---")
	featuresAndLabel_data.show(5, truncate = false)

	val Array(train, test) = featuresAndLabel_data.randomSplit(Array(0.8, 0.2))

	//	val rf = new RandomForestClassifier()
	//		.setLabelCol("label")
	//		.setFeaturesCol("features")
	//		.setNumTrees(10)
	//  	.setMaxBins(8000)
	//
	//	val model = rf.fit(train)
	//
	//	val predictions = model.transform(test)
	//
	//	predictions.show(10, truncate = false)
	//
	//	val evaluator = new MulticlassClassificationEvaluator()
	//		.setLabelCol("label")
	//		.setPredictionCol("prediction")
	//		.setMetricName("accuracy")
	//	val accuracy = evaluator.evaluate(predictions)
	//	println("Test Accuracy = " + accuracy)


	val lr = new LogisticRegression()
		.setMaxIter(10)
		.setRegParam(0.3)
		.setElasticNetParam(0.8)

	val model = lr.fit(train)

	val trainingSummary = model.summary

	// Obtain the objective per iteration.
	val objectiveHistory = trainingSummary.objectiveHistory
	println("objectiveHistory:")
	objectiveHistory.foreach(loss => println(loss))

	// Obtain the metrics useful to judge performance on test data.
	// We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
	// binary classification problem.
	val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

	// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
	val roc = binarySummary.roc
	roc.show(10, truncate = false)
	println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

	// Set the model threshold to maximize F-Measure
	val fMeasure = binarySummary.fMeasureByThreshold
	val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
	val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
		.select("threshold").head().getDouble(0)
	model.setThreshold(bestThreshold)

	spark.stop()
}