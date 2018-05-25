import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.mllib._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

object Classification {
  def main(args: Array[String]): Unit = {

    // create a spark context
    val sc = new SparkContext(new SparkConf()
      .setAppName("Classification")
      .setMaster("yarn"))

    val testDataFile = args(0)
    val modelPath = args(1)
    val output = args(2)

    // Load the Random Forest Models from file
    val rfmodel1 = RandomForestModel.load(sc, modelPath+"/model1")
    val rfmodel2 = RandomForestModel.load(sc, modelPath+"/model2")
    val rfmodel3 = RandomForestModel.load(sc, modelPath+"/model3")

    // read validation data and convert to labelled point
    val testData = sc.textFile(testDataFile)
      .map(row=>row.split(","))
      .map(row=>new LabeledPoint(0.0,
        Vectors.dense(row.take(row.length-1).map(row=>row.toDouble))))
      .cache()

    // predict the label for input data and save the labels to file
    val rftest = testData.map(point=>predictRandomForest(point,rfmodel1,rfmodel2,rfmodel3).toInt).cache()
    testData.unpersist()
    rftest.repartition(1).saveAsTextFile(output)
    rftest.unpersist()

  }

  /*
  Prediction function for random forest classifier
   */
  def predictRandomForest(point: LabeledPoint, model1: RandomForestModel, model2: RandomForestModel, model3: RandomForestModel): Double = {
    val prediction1 = model1.predict(point.features)
    val prediction2 = model2.predict(point.features)
    val prediction3 = model3.predict(point.features)

    val predictionSum = prediction1+prediction2+prediction3
    if(predictionSum>=2)
      return 1
    else
      return 0
  }
  
}
