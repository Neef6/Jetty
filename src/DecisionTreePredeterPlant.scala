/**
  * Created by apple on 17/5/7.
  */
package com.cloudera.datascience.rdf

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreePredeterPlant {




  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "RDF", new SparkConf())
    val rawData = sc.textFile("/Users/apple/Documents/inputPath/SparkMLib/04/covtype1.data")

    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      //init 返回除最后一个值外的其他值
      val label = values.last - 1 //decisionTree start from zero
      LabeledPoint(label, featureVector)
    }

    //训练集，交叉检验集，测试集
    // Split into 80% train, 10% cross validation, 10% test
    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    simpleDecisionTree(trainData, cvData)

    randomClassifier(trainData, cvData)

    evaluate(trainData, cvData, testData)
    evaluateCategorical(rawData)
    evaluateForest(rawData)
  }

  def simpleDecisionTree(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    //build a simple tree，max deep ：4，max  barrel：100
    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "gini", 4, 100)
    val metrics = getMetrics(model, cvData)
    (0 until 7).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)
  }

  def getMetrics(model: DecisionTreeModel, cvData: RDD[LabeledPoint]): MulticlassMetrics = {
    val predicitionsAndLabels = cvData.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predicitionsAndLabels)
  }


  //类别在训练，cv集合中出现的概率相乘，再相加，然后就得到一个对准确度的评估
  def randomClassifier(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    //symbol zip      将2个rdd对应位置上的值组成一个pair数组
    val accuracy = trainPriorProbabilities.zip(cvPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println(accuracy)
  }

  def classProbabilities(trainData: RDD[LabeledPoint]): Array[Double] = {
    //count (category,sampleNum) in data
    val countsByCategory = trainData.map(_.label).countByValue
    //对类别的样本数进行排序并取出样本数
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

  def evaluate(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint],
               testData: RDD[LabeledPoint]): Unit = {
    val evaluations = for (impurity <- Array("gini", "entropy");
    depth <- Array(1, 20);
    bins <- Array(10, 300)
    )
    yield
    {
      val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), impurity, depth, bins)
      val accuracy = getMetrics(model, cvData).precision((imp))
    }


  }


}