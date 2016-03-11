package spark.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import java.util.{Calendar, TimeZone}

object SparkSaveDataFrame {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSaveDataFrame").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
  //import sqlContext.implicits._

    val anotherPeopleRDD = sc.parallelize(
      s"""
         |{"dt":"${nowDateTime()}"}}
       """.stripMargin :: Nil)
    val DummyDF = sqlContext.read.json(anotherPeopleRDD)
    saveDF(DummyDF)

    println("END")
    System.exit(0)
  }

  def saveDF(DummyDF: DataFrame): Unit = {
    val n = nowDateTime()
    DummyDF.write.save(s"/home/m/temp/${n}/")
  }

  def nowDateTime(): String = {
    val now = Calendar.getInstance()
    now.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))

    val now_year = now.get(Calendar.YEAR)
    val now_month = f"${now.get(Calendar.MONTH)+1}%02d"
    val now_day = f"${now.get(Calendar.DAY_OF_MONTH)}%02d"
    val now_hour = f"${now.get(Calendar.HOUR_OF_DAY)}%02d"
    val now_minute = f"${now.get(Calendar.MINUTE)}%02d"
    val now_second = f"${now.get(Calendar.SECOND)}%02d"
    val now_millis = f"${now.get(Calendar.MILLISECOND)}%03d"

    val s = s"${now_year}-${now_month}-${now_day} ${now_hour}:${now_minute}:${now_second}-${now_millis}"
    println(s)

    return s
  }
}