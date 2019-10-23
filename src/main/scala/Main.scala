/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Main {
  def main(args: Array[String]) {
    // create needed paths depending on user
    val mdyePath = "file:///C:/rattrapages/sparkGuessMyCLick/src/main/resources/data-students.json"
    // val wregPath = "data-students.json"


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GuessMyClick")
      .config("","")
      .getOrCreate()
    val df = spark.read.json(mdyePath)

    import spark.implicits._

    val interests = df.select(col("interests")).limit(50)
    val df_tranform = interests.as[String].rdd
    println("-------------------"+df_tranform.count()+"----------------")
    val final_map = df_tranform.collect()
    val interests_list = final_map.toList.filter(el => el!=null).flatMap(el => el.split(",")).distinct
    println(interests_list)


  }
}