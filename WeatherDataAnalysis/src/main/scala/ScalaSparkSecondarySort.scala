import org.apache.spark._

object ScalaSparkSecondarySort {

  /** Main function */
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

    val weatherData = sc.textFile("input/")
      .map(extractData)
      .cache()

    val finalResult = weatherData.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
      .map(row => (row._1, row._2._2 / row._2._1, row._2._4 / row._2._3))
      .sortBy(row => row._1)
      .map(row => (row._1._1, ("<" + row._1._2 + " , " + row._2 + " , " + row._3 + ">")))
      .reduceByKey(_ + _)
      .collect

    finalResult.map(println)

    sc.stop()
  }

  def extractData(line: String): ((String, String), (Long, Double, Long, Double)) = {
    val parts = line.split(",")
    val stationId = parts(0)
    val year = parts(1).substring(0,4)
    val tempType = parts(2)
    val temperature = parts(3).toDouble

    if(tempType == "TMIN") ((stationId,year), (1, temperature, 0, 0))
    else ((stationId,year), (0, 0, 1, temperature))
  }
}
