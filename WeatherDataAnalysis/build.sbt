name := "WeatherDataAnalysis"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "compile" withSources()
  )
}