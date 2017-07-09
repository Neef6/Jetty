package cn.edu.nj


import java.util.Properties

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.ling.CoreAnnotations._
import org.apache.spark.ml.linalg.Matrix

import scala.collection.mutable.ArrayBuffer

/**
  * Created by apple on 17/7/7
  */
object Lsa {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("lsa")
    val sc = new SparkContext(conf)

    val numTerms = 500
    val sampleSize = 0.1

    val (termDocMatrix, termIds, docIds, idfs) = preproccessing(sampleSize, numTerms, sc)
    termDocMatrix.cache()

    val mat = new RowMatrix(termDocMatrix)
    val svd = mat.computeSVD(k, computeU = true)

    println("Singular values:" + svd.s)
    val topConceptTrems = topTermsInsTopConcepts(svd, 10, 10, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 10, 10, docIds)
    for ((terms, docs) <- topConceptTrems.zip(topConceptDocs)) {
    }

    //词行合并
    def createNLPPieline(): StanfordCoreNLP = {
      val props = new Properties()
      props.put("annotators", "tokenize,ssplit,pos,lemma")
      new StandfordCoreNLP(props)
    }

    def isOnlyLetters(str: String): Boolean = {
      str.forall(c=>Character.isLetter(c))
    }

    def plainTextToLemmas(text:String,stopWords:Set[String],pipline:StanfordCoreNLP):Seq[String]={
      val doc = new Annotation(text)




    }

    def preproccessing(samplesize: Double, numTerms: Int, sc: SparkContext)
    : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
      val plainText = pages.filter(_ != null).flatMap()

      val stopWords = sc.broadcast(loadStopWords("stopwords.txt")).value

      val lemmatized = plainText.mapPartitions(iter =>
      val pipeline = createNLPPieline()
      iter.map { case (title, contents) => (title, plainTextToLemmas(contents, stopWords, pipeline)) }
      )

      val filtered = lemmatized.filter(_._2.size > 1)

      documentTermMatrix(filtered, stopWords, numTerms, sc)
    }

    def topTermsInTopConcepts(svd: SinglarValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                              termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
      val v = svd.v
      val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
      val arr = v.toArray
      for (i <- util numConcepts) {
        val offs = i * v.numRows
        val termWeigths = arr.slice(offs, offs + v.numRow).zipWithIndx
        val sorted = termWeigths.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map { case (score, id) => (termIds(id), score) }
      }
      topTerms
    }

    def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                             numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {


      val u = svd.U
      val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
      for (i <- 0 until numConcepts) {


      }


    }


  }

}
