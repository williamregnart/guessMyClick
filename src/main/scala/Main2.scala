package rtb

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}

object Main2 extends App{

	val context = SparkSession
		.builder()
		.appName("GuessMyClick")
		.master("local")
		.getOrCreate()

	context.sparkContext.setLogLevel("WARN")

	import org.apache.spark.sql.functions._

	val data = context.read.json("data-students.json")
		.select("appOrSite", "network", "type", "publisher","size", "label", "interests", "user")

	val refined_data = data
		.withColumn("label",
			when(col("label") === true, 1)
				.otherwise(0)
		)
		.withColumn("network",
			Cleaner.clean_network(data("network"))
		)
		.withColumn("newSize",
			when(data("size").isNotNull, concat_ws(" ", data("size")))
				.otherwise("Unknown")
		)
		.drop("size")

	val cleaned_data = refined_data
		.withColumn("interests",
			when(refined_data("interests").isNotNull, Cleaner.udf_renameInterestByRow(refined_data("interests")))
				.otherwise("null")
		)
  	.drop("user")

	val columns = cleaned_data.drop("label").columns

	val indexers = columns.map(
		col => new StringIndexer()
			.setInputCol(col)
			.setOutputCol("indexed_" + col)
			.setHandleInvalid("skip")
	)

	val encoders = columns.map(
		col => new OneHotEncoderEstimator()
			.setInputCols(Array("indexed_" + col))
			.setOutputCols(Array("encoded_" + col))
	)

	val pipeline = new Pipeline()
		.setStages(indexers ++ encoders)

	val encoded_data = pipeline.fit(cleaned_data).transform(cleaned_data)

	val encoded_columns = columns.map(col => "encoded_" + col)

	val vector_column_assembler = new VectorAssembler()
		.setInputCols(encoded_columns)
		.setOutputCol("features")

	vector_column_assembler.transform(encoded_data)

	val features_label_data = vector_column_assembler.transform(encoded_data).select("features", "label")

	println("---data---")
	data.show(10, truncate = false)
	println("---cleaned data---")
	cleaned_data.show(10, truncate = false)
	println("---encoded data---")
	encoded_data.show(10, truncate = false)
	println("---features label data---")
	features_label_data.show(10, truncate = false)

	val Array(trainData, testData) = features_label_data.randomSplit(Array(0.8, 0.2))

	val lr = new LogisticRegression()
		.setMaxIter(10)
		.setFeaturesCol("features")
		.setLabelCol("label")

	val paramGrid = new ParamGridBuilder()
		.addGrid(lr.regParam, Array(0.1, 0.01))
		.build()

	val cv = new CrossValidator()
		.setEstimator(lr)
		.setEvaluator(new BinaryClassificationEvaluator)
		.setEstimatorParamMaps(paramGrid)
		.setNumFolds(5)
  	.setParallelism(4)

	val model = cv.fit(trainData)

	model.write.overwrite().save("model")

	val predictions = model.transform(testData)

	val lp = predictions.select( "label", "prediction")
	val counttotal = predictions.count()
	val correct = lp.filter(col("label") === col("prediction")).count()
	val wrong = lp.filter(not(col("label") === col("prediction"))).count()
	val true_0 = lp.filter(col("prediction") === 0.0).filter(col("label") === col("prediction")).count()
	val false_0 = lp.filter(col("prediction") === 0.0).filter(not(col("label") === col("prediction"))).count()
	val true_1 = lp.filter(col("prediction") === 1.0).filter(col("label") === col("prediction")).count()
	val false_1 = lp.filter(col("prediction") === 1.0).filter(not(col("label") === col("prediction"))).count()

	val ratioWrong=wrong.toDouble/counttotal.toDouble
	val ratioCorrect=correct.toDouble/counttotal.toDouble
	val precision = true_1.toDouble / (true_1.toDouble + false_1.toDouble)
	val recall = true_1.toDouble / (true_1.toDouble + false_0.toDouble)

	println("total predictions: " + counttotal)
	println("total correct: " + correct)
	println("total wrong: " + wrong)
	println("ratio correct: " + ratioCorrect)
	println("ratio wrong: " + ratioWrong)
	println("total true 0: " + true_0)
	println("total false 0: " + false_0)
	println("total true 1: " + true_1)
	println("total false 1: " + false_1)
	println("precision: " + precision)
	println("recall: " + recall)

	val evaluator = cv.getEvaluator
	println(evaluator.asInstanceOf[BinaryClassificationEvaluator].getMetricName + " : " + evaluator.evaluate(predictions))

	context.stop()
}