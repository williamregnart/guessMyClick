package rtb

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, split, udf, when}
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.matching.Regex


case class Cleaner() {


  def selectValueFromGivenCount(data: DataFrame, limit: String, column: String) = {
    data.groupBy(column).count()
      .filter("count "+limit)
      .sort("count")
      .select(column)
      //.show(200)
      .collect()
  }
}

object Cleaner{

  def clean_network: UserDefinedFunction = {
    udf {(s: String) =>
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
  udf (
    (s: String) => {
        val regex = new Regex("-(.*)");
        val arrayOfInterests = s.split(',')
         .map(interest => regex.replaceAllIn(interest, ""))
        arrayOfInterests.mkString(" ");
      }
    )
  }
}
