import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat_ws, not, udf, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.matching.Regex

object GuessMyClick {

	def processData(data: DataFrame, showSteps: Boolean): DataFrame = {
		def clean_network: UserDefinedFunction = {
			udf { (s: String) =>
				val ethernet = new Regex("1(.*)")
				val wifi = new Regex("2(.*)")
				val cell1 = new Regex("3(.*)")
				val cell2 = new Regex("4(.*)")
				val cell3 = new Regex("5(.*)")
				val cell4 = new Regex("6(.*)")

				s match {
					case ethernet(x) => "Ethernet"
					case wifi(x) => "Wifi"
					case cell1(x) => "Unknown Cellular"
					case cell2(x) => "2G Cellular"
					case cell3(x) => "3G Cellular"
					case cell4(x) => "4G Cellular"
					case _ => "Unknown"
				}
			}
		}

		def renameInterestByRow: UserDefinedFunction = {
			udf(
				(s: String) => {
					val regex = new Regex("-(.*)");
					val arrayOfInterests = s.split(',')
						.map(interest => regex.replaceAllIn(interest, ""))
					arrayOfInterests.mkString(" ");
				}
			)
		}

		val refined_data = data
			.withColumn("label",
				when(col("label") === true, 1)
					.otherwise(0)
			)
			.withColumn("newNetwork",
				clean_network(data("network"))
			)
			.withColumn("newSize",
				when(data("size").isNotNull, concat_ws(" ", data("size")))
					.otherwise("Unknown")
			)
			.withColumn("newType", col("type"))
			.withColumn("newInterests",
				when(data("interests").isNotNull, renameInterestByRow(data("interests")))
					.otherwise("null")
			)

		val columns = refined_data.select("appOrSite", "publisher", "newNetwork", "newSize", "newType", "newInterests").columns

		val indexers = columns.map(
			col => new StringIndexer()
				.setInputCol(col)
				.setOutputCol("indexed_" + col)
				.setHandleInvalid("keep")
		)

		val encoders = columns.map(
			col => new OneHotEncoderEstimator()
				.setInputCols(Array("indexed_" + col))
				.setOutputCols(Array("encoded_" + col))
		)

		val pipeline = new Pipeline()
			.setStages(indexers ++ encoders)

		val encoded_data = pipeline.fit(refined_data).transform(refined_data)

		val encoded_columns = columns.map(col => "encoded_" + col)
		println(encoded_columns.mkString(" "))

		val vector_column_assembler = new VectorAssembler()
			.setInputCols(encoded_columns)
			.setOutputCol("features")

		val vector_column_assembled_data = vector_column_assembler.transform(encoded_data)

		if (showSteps) {
			println("---data---")
			data.show(10, truncate = false)
			println("---refined data---")
			refined_data.show(10, truncate = false)
			println("---encoded data---")
			encoded_data.show(10, truncate = false)
			println("---vector assembled data---")
			vector_column_assembled_data.show(10, truncate = false)
		}

		vector_column_assembled_data
	}

	def constructModel(): CrossValidator = {
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
		cv
	}

	def trainModel(model: CrossValidator, data: Dataset[Row]): CrossValidatorModel = {
		val trainedModel = model.fit(data)
		trainedModel
	}

	def predict(model: CrossValidatorModel, data: Dataset[Row]): DataFrame = {
		val predictions = model.transform(data)
		predictions
	}

	def saveModel(model: CrossValidatorModel): Unit = {
		model.write.overwrite().save("model")
	}

	def loadModel(path: String): CrossValidatorModel = {
		val model = CrossValidatorModel.load(path)
		model
	}

	def evaluateModel(model: CrossValidator, predictions: DataFrame): Unit = {
		val lp = predictions.select("label", "prediction")
		val counttotal = predictions.count()
		val correct = lp.filter(col("label") === col("prediction")).count()
		val wrong = lp.filter(not(col("label") === col("prediction"))).count()
		val true_0 = lp.filter(col("prediction") === 0.0).filter(col("label") === col("prediction")).count()
		val false_0 = lp.filter(col("prediction") === 0.0).filter(not(col("label") === col("prediction"))).count()
		val true_1 = lp.filter(col("prediction") === 1.0).filter(col("label") === col("prediction")).count()
		val false_1 = lp.filter(col("prediction") === 1.0).filter(not(col("label") === col("prediction"))).count()

		val ratioWrong = wrong.toDouble / counttotal.toDouble
		val ratioCorrect = correct.toDouble / counttotal.toDouble
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

		val evaluator = model.getEvaluator
		println(evaluator.asInstanceOf[BinaryClassificationEvaluator].getMetricName + " : " + evaluator.evaluate(predictions))
	}

	def main(args: Array[String]): Unit = {
		if (args.length == 0) {
			println("usage: guessMyClick dataset.json [--TRAIN] [--DEBUG]")
		} else {
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)

			val debug = args.contains("--DEBUG")
			val train = args.contains("--TRAIN")

			val context = SparkSession
				.builder()
				.appName("GuessMyClick")
				.master("local")
				.getOrCreate()

			context.sparkContext.setLogLevel("WARN")

			println("--loading data--")
			val data = context.read.json(args(0))

			println("--data loaded--")

			println("--processing data--")
			val processedData = processData(data, showSteps = debug)
			println("--data processed--")

			val model = constructModel()

			var predictions: DataFrame = null

			if (train) {
				println("--training model--")
				val Array(trainData, testData) = processedData.randomSplit(Array(0.8, 0.2))
				val trainedModel = trainModel(model, trainData)
				saveModel(trainedModel)
				println("--model  trained--")

				println("--predicting--")
				predictions = predict(trainedModel, testData)

				println("--evaluating--")
				evaluateModel(model, predictions)
			} else {
				println("--loading model--")
				val trainedModel = loadModel("model")
				println("--model loaded--")

				println("--predicting--")
				predictions = predict(trainedModel, processedData)
				println(s"predicted ${predictions.count()} values")
			}

			predictions.show(10, truncate = false)

			val labeled_data = predictions
  				.select("appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "media", "network", "os", "publisher", "size", "timestamp", "type", "user", "prediction")
  				.withColumn("label", col("prediction").cast("Boolean"))
  				.drop("prediction")

			val ordered_labeled_data = labeled_data.select("label", "appOrSite", "bidfloor", "city", "exchange", "impid", "interests", "media", "network", "os", "publisher", "size", "timestamp", "type", "user")

			ordered_labeled_data.show(10, truncate = false)

			val stringify = udf((vs: Seq[String]) => vs match {
				case null => null
				case _    => s"""[${vs.mkString(",")}]"""
			})

			ordered_labeled_data
				.withColumn("size", stringify(col("size")))
				.repartition(1)
				.write.option("header", "true")
				.csv("output")

			context.stop()
		}
	}
}