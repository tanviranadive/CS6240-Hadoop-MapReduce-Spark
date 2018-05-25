import org.apache.spark._

/** Main class */
object WeatherDataAnalysis {

  /** Main function */
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

    val weatherData = sc.textFile("input/1763.csv")
      .map(extractData)
      .cache()

    val tminData = weatherData.filter(row => row._2._1 == "TMIN").map(row => (row._1, row._2._2))

    val tmaxData = weatherData.filter(row => row._2._1 == "TMAX").map(row => (row._1, row._2._2))

    val tminResult = tminData.groupByKey().mapValues(vals => vals.toList.sum/vals.toList.size)

    val tmaxResult = tmaxData.groupByKey().mapValues(vals => vals.toList.sum/vals.toList.size)

    val finalResult = tminResult.join(tmaxResult).collect()

    println(finalResult.mkString(" | "))

    sc.stop()
  }

  def extractData(line: String): (String, (String, Double)) = {
    val parts = line.split(",")
    val stationId = parts(0)
    val tempType = parts(2)
    val temperature = parts(3).toDouble
    (stationId, (tempType, temperature))
  }
}