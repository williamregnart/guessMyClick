package sample
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.linalg._

object sparkMain extends App {
    val path = "file:///C:/rattrapages/sparkGuessMyCLick/src/main/resources/data-students.json"

    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Guess My Click").getOrCreate()
    import sparkSession.implicits._


    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json(path)

    df.show(20, 15)

    //df.select(df.columns.map(c => countDistinct(col(c)).alias(c)): _*).show

    //df.withColumn("which_os", explode(array(col("os")))).select(col("which_os")).distinct().show
    //df.withColumn("which_size", explode(array(col("size")))).select(col("which_size")).distinct().show
    //df.withColumn("which_interests", explode(array(col("interests")))).select(col("which_interests")).distinct().show
    //df.withColumn("which_type", explode(array(col("type")))).select(col("which_type")).distinct().show


    val df_to_reshape = df
      .withColumn("is_app",
          when(df.col("appOrSite").equalTo("site"), 1)
            .otherwise(when(df.col("appOrSite").equalTo("app"), 0)
            .otherwise(-1)))
      .drop(df.col("appOrSite"))
      .drop(df.col("bidfloor"))
      //.drop(col("city"))
      .drop(df.col("exchange"))
      .drop(df.col("impid"))
      .withColumn("has_clicked",
          when(df.col("label").equalTo("false"), 0)
            .otherwise(when(df.col("label").equalTo("true"), 1)
            .otherwise(-1))
      )
      .withColumn("is_media_d4769",
          when(df.col("media").contains("d476955e1ffb"), 1)
      .otherwise(0))
      .drop(col("label"))
      .drop(df.col("publisher"))
      .drop(col("user"))

      df_to_reshape.show(20, 15)

    val data = df_to_reshape.select(col("is_media_d4769"), col("has_clicked"))
    val contingency_df= data.stat.crosstab("is_media_d4769", "has_clicked")
    val contingency_df_no_first = contingency_df.drop(col(contingency_df.columns.head))
    val contingency_rdd = contingency_df_no_first.as[(Double, Double)].rdd

    val values = contingency_rdd.map(row => List(row._1, row._2)).flatMap(row=> row).collect()

    val row_count = contingency_df_no_first.count().toInt
    val col_count = contingency_df_no_first.columns.size

    val matrix : Matrix = Matrices.dense(row_count, col_count, values)

    println(Statistics.chiSqTest(matrix))


}