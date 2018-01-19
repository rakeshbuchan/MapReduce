package imagepredictionproject

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
 * Training a RandomForest model with training data and validating it with test
 * data setting hyper parameters that control the performance of the training 
 * model
 */
object TrainingModelWithInputPartitioning {
  
  def main(args: Array[String]) : Unit = {
    
//    val conf = new SparkConf().setAppName("Image Training Partitioned Model").setMaster("local[*]");
    val conf = new SparkConf().setAppName("Image Training Partitioned Model").setMaster("yarn");
    val sc = new SparkContext(conf);
    
    /**
     * Partitioning input files into training and testing data for the model
     */
    val trainingInputContent = sc.textFile(args(0) + "/training_data");
    val testingInputContent = sc.textFile(args(0) + "/testing_data");
         
    /**
     * Creating an RDD of LabeledPoint to provide as input to the training
     * model  
     */
    val trainingData = trainingInputContent.map(createLabeledData);
    
    val testData = testingInputContent.map(createLabeledData);
    
    /**
     * Setting hyper-parameters as input to RandomForsest training model
     */
    val numClasses = 2;
    
    var categoricalFeaturesInfo = Map[Int,Int]();
    
    val numTrees = 40;
    
    val featureSubsetStrategy = "auto"; //Let the algorithm choose
    
    val impurity = "gini";
    
    val maxDepth = 15;
    
    val maxBins = 140;
    
    /**
     * Train a RandomForest model
     */
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, 
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins);
    
    
    val labelAndPreds = testData.map{
      point => 
        val prediction = model.predict(point.features);
        (point.label, prediction)
    }
    
    /**
     * Evaluate model on test instances and compute test error
     */
//    val testError = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count();
//    
//    println("Learned classification forest model:\n" + model.toDebugString);
//    
//    println("Test Error = " + testError);
    
    /**
     * Metrics to obtain model precision
     */
//    val metrics = new MulticlassMetrics(labelAndPreds);
//    
//    val precision = Array(metrics.precision);
//    
//    sc.parallelize(precision, 1).saveAsTextFile(args(1) + "/precision");
    
//    labelAndPreds.repartition(1).saveAsTextFile(args(1) + "/training_output");
    
    /**
     * Save training model
     */
    model.save(sc, args(1) + "/model/myRandomForestClassificationModel");

    sc.stop();
    
  }
  
  /**
   * createLabeledData transforms each input record to a LabeledPoint to 
   * provide as a pair of label and feature as input to train the model
   */
   def createLabeledData = {
    (record : String) =>
      
      val recordSplits1 = record.split(",");
      val recordSplits = recordSplits1.map(_.toDouble);
      val featureVector = Vectors.dense(recordSplits.init);
      val label = recordSplits.last;
      LabeledPoint(label, featureVector); 
      
  }
}