

lazy val root = (project in file(".")).
  settings(
    name := "PageRank",
    version := "0.1",
    mainClass in Compile := Some("PageRank")
  )


scalaVersion := "2.11.6"

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "compile" withSources()
  )
}
