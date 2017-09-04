
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkApplication extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val logger = Logger.getLogger("Spark")

  val sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val pagecount = sc.textFile("/home/knoldus/Downloads/Spark-Assignment_01/src/main/resources/pagecounts-20151201-220000").persist()

  def getTenRecords()  ={
    pagecount.take(10).foreach(println(_))
  }
//  getTenRecords()

  def getTotalRecords() = {
    logger.info(pagecount.count())
  }

 // getTotalRecords() // total count is 7598006

  def onlyEnglishPage() = {
    pagecount.map(line => line.split(" ")).filter(elements => elements(0) =="en").map(eng => eng.toList.toString())
  }

  def countEnglishPage() ={
    logger.info(onlyEnglishPage().count)
  }

  def request2lakh() = {
   val twoLakh = pagecount.map { data =>
      val line = data.split(" ")
      (line(1), line(2).toInt)
    }.reduceByKey(_ + _).filter{ case(_,count) => count >200000}

    twoLakh.collect().foreach(println)
  }
}
