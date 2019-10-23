/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD

object SimpleApp {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("GuessMyClick")
      .config("","")
      .getOrCreate()
    val df = spark.read.json("data-students.json")

    import spark.implicits._

    val interests = df.select(col("interests")).limit(50)
    val df_tranform = interests.as[String].rdd
    println("-------------------"+df_tranform.count()+"----------------")
    val final_map = df_tranform.collect()
    val interests_list = final_map.toList.map(element => element)
    println(interests_list)


  }



  def createColumn(dataframe:DataFrame, new_col_name:String, example_col_name:String, valueIs:String):DataFrame= {
    dataframe.withColumn(new_col_name,when(dataframe(example_col_name) === valueIs,1).otherwise(0))
  }
}