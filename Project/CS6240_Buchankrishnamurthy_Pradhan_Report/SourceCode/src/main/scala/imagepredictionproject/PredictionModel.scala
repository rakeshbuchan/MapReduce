package imagepredictionproject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
 * Predicting the label of unlabeled data providing its features as input to
 * the trained model
 */
object PredictionModel {
  
  def main(args : Array[String]) : Unit = {
    
//    val conf = new SparkConf().setAppName("Prediction Model").setMaster("local[*]");
    val conf = new SparkConf().setAppName("Prediction Model").setMaster("yarn");
    val sc = new SparkContext(conf);
    
    val inputContent = sc.textFile(args(0) + "/prediction_data");
    
    /**
     * Creating an RDD of LabeledPoint to provide as input to the training
     * model  
     */    
    val preprocessedInputForPrediction = inputContent.map(createLabeledData);
    
    /**
     * Loading the training model    
     */
    val savedTrainingModel = RandomForestModel.load(sc, args(1) + "/model/myRandomForestClassificationModel");
    
     /**
     * Make predictions on incoming records of unlabeled data
     */
    val labelAndPreds = preprocessedInputForPrediction.map{
      point => 
        val prediction = savedTrainingModel.predict(point.features);
        (prediction.toInt)
    }
    
    labelAndPreds.repartition(1).saveAsTextFile(args(1) + "/prediction_output");
    
    sc.stop();
    
    
  }
  
  /**
   * createLabeledData transforms each input record to a LabeledPoint to 
   * provide as a pair of label and feature as input to the trained model
   */
  def createLabeledData = {
    (record : String) =>
      
      val recordSplits = record.split(",");
      val features = recordSplits.init.map(_.toDouble);
      val featureVector = Vectors.dense(features);
      val label = 0.0;
      LabeledPoint(label, featureVector);    
  }
}