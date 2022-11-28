


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{ParamGryearBuilder, crossvalidator}
import org.apache.spark.sql.{Row, SQLContext}

val conf = new SparkConf().setAppName("CrossValidator")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext._


val training = sparkContext.parallelize(Seq(
//training set)))


val tokenizer = new Tokenizer()
  .setInputCol("text")
  .setOutputCol("words")
val hashingTF = new HashingTF()
  .setInputCol(tokenizer.getOutputCol)
  .setOutputCol("features")
val lr = new LogisticRegression()
  .setMaxIter(10)
val pipeline = new Pipeline()
  .setStages(Array(tokenizer, hashingTF, lr))


val crossval = new crossvalidator()
  .setEstimator(pipeline)
  .setEvaluator(new BinaryClassificationEvaluator)


val paramGrid = new ParamGryearBuilder()
  .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
  .addGrid(lr.regParam, Array(0.1, 0.01))
  .build()
crossval.setEstimatorParamMaps(paramGrid)
crossval.setNumFolds(3) 


val cvModel = crossval.fit(training)

val lrModel = cvModel.bestModel

val test = sparkContext.parallelize(Seq(
//test set)))


cvModel.transform(test)
  .select('year, 'text, 'emissions, 'prediction)
  .collect()
  .foreach { case Row(year: Long, text: String, emissions: Double, prediction: Double) =>
  println("(" + year + ", " + text + ") --> emissions=" + emissions + ", prediction=" + prediction)
}