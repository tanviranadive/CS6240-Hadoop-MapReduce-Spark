
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.mllib._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD

object Classification {
  def main(args: Array[String]): Unit = {

    // create a spark context
    val sc = new SparkContext(new SparkConf()
      .setAppName("Classification")
      .setMaster("yarn"))

    val trainingDataFile = args(0)
    val validationDataFile = args(1)
    val modelPath = args(2)
    val accuracyOutput = args(3)

    // read training data and convert to labelled point
    val parsedData = sc.textFile(trainingDataFile+"/*.csv", sc.defaultParallelism)
      .map(row=>row.split(","))
      .map(row=>new LabeledPoint(row.last.toDouble,
        Vectors.dense(row.take(row.length-1).map(row=>row.toDouble))))
       .cache()

    // train three Random Forest Models using random sampling of labelled data
    val rfmodel1 = trainRandomForest(parsedData.sample(true,0.5))
    val rfmodel2 = trainRandomForest(parsedData.sample(true,0.5))
    val rfmodel3 = trainRandomForest(parsedData.sample(true,0.5))

    // Save the models to file
    rfmodel1.save(sc,modelPath+"/model1")
    rfmodel2.save(sc,modelPath+"/model2")
    rfmodel3.save(sc,modelPath+"/model3")

    parsedData.unpersist()


    // read validation data and convert to labelled point
    val validationData = sc.textFile(validationDataFile)
      .map(row=>row.split(","))
      .map(row=>new LabeledPoint(row.last.toDouble,
        Vectors.dense(row.take(row.length-1).map(row=>row.toDouble))))
      .cache()


    // Calculate the accuracy using count of correctly predicted labelled points
    val rfvalidation = validationData.filter(point=>predictRandomForest(point,rfmodel1,rfmodel2,rfmodel3)==point.label)
    val validationDataCount = validationData.count()
    validationData.unpersist()
    val predictionCount = rfvalidation.count()


    val accuracy = (predictionCount.toFloat/validationDataCount.toFloat)*100

    println("Accuracy% => "+accuracy)
    val result = sc.parallelize(Seq(accuracy))

    // Save the accuracy
    result.saveAsTextFile(accuracyOutput)

  }

  /*
  Train Random Forest model
   */
  def trainRandomForest(trainingData: RDD[LabeledPoint]): RandomForestModel={

    val categoricalFeaturesInfo = Map[Int, Int]()
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val impurity = "gini"
    val numTrees = 15
    val numClasses = 2
    val featureSubsetStrategy = "auto"
    val maxDepth = 5
    val maxBins = 50

    val model = RandomForest.trainClassifier(trainingData,
      numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy,
      impurity, maxDepth, maxBins)

    return model
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
