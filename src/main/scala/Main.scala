import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
		.withColumn("type",
			when(data.col("type").isNull, -1)
				.otherwise(data.col("type"))
		)
		.drop("user")
		.withColumn("has_iab1",
			col("interests").rlike(rlikeFor("IAB1"))
		)
		.withColumn("has_iab2",
			col("interests").rlike(rlikeFor("IAB2"))
		)
		.withColumn("has_iab3",
			col("interests").rlike(rlikeFor("IAB3"))
		)
		.withColumn("has_iab4",
			col("interests").rlike(rlikeFor("IAB4"))
		)
		.withColumn("has_iab5",
			col("interests").rlike(rlikeFor("IAB5"))
		)
		.withColumn("has_iab6",
			col("interests").rlike(rlikeFor("IAB6"))
		)
		.withColumn("has_iab7",
			col("interests").rlike(rlikeFor("IAB7"))
		)
		.withColumn("has_iab8",
			col("interests").rlike(rlikeFor("IAB8"))
		)
		.withColumn("has_iab9",
			col("interests").rlike(rlikeFor("IAB9"))
		)
		.withColumn("has_iab10",
			col("interests").rlike(rlikeFor("IAB10"))
		)
		.withColumn("has_iab11",
			col("interests").rlike(rlikeFor("IAB11"))
		)
		.withColumn("has_iab12",
			col("interests").rlike(rlikeFor("IAB12"))
		)
		.withColumn("has_iab13",
			col("interests").rlike(rlikeFor("IAB13"))
		)
		.withColumn("has_iab14",
			col("interests").rlike(rlikeFor("IAB14"))
		)
		.withColumn("has_iab15",
			col("interests").rlike(rlikeFor("IAB15"))
		)
		.withColumn("has_iab16",
			col("interests").rlike(rlikeFor("IAB16"))
		)
		.withColumn("has_iab17",
			col("interests").rlike(rlikeFor("IAB17"))
		)
		.withColumn("has_iab18",
			col("interests").rlike(rlikeFor("IAB18"))
		)
		.withColumn("has_iab19",
			col("interests").rlike(rlikeFor("IAB19"))
		)
		.withColumn("has_iab20",
			col("interests").rlike(rlikeFor("IAB20"))
		)
		.withColumn("has_iab21",
			col("interests").rlike(rlikeFor("IAB21"))
		)
		.withColumn("has_iab22",
			col("interests").rlike(rlikeFor("IAB22"))
		)
		.withColumn("has_iab23",
			col("interests").rlike(rlikeFor("IAB23"))
		)
		.withColumn("has_iab24",
			col("interests").rlike(rlikeFor("IAB24"))
		)
		.withColumn("has_iab25",
			col("interests").rlike(rlikeFor("IAB25"))
		)
		.withColumn("has_iab26",
			col("interests").rlike(rlikeFor("IAB26"))
		)


	println("---refined data columns---")
	refined_data.columns.foreach(println)

	println("---refined data preview---")
	refined_data.show(20, truncate = false)

	//	println("---interests values---")
	//	val pattern = """IAB\d{1,2}""".r
	//	refined_data.select("interests").as[String].collect().filter(l => l != null).flatMap(_.split(",")).filter(_.startsWith("IAB")).map(e => pattern.findAllIn(e).mkString).distinct.sorted.foreach(println)

}