package cn.edu.nj


import java.text.Annotation
import java.util.Properties
import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector,
SparseVector => BSparseVector}


import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.ling.CoreAnnotations._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
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

    def preproccessing(samplesize: Double, numTerms: Int, sc: SparkContext)
    : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
      val pages = readFile("pag.txt", sc).sample(false, sampleSize, 1L);
      val plainText = pages.filter(_ != null).flatMap(x => x)
      //Some((plainText.getTitle, plainText.getContent))

      val stopWords = sc.broadcast(loadStopWords("stopwords.txt")).value

      val lemmatized: RDD[Seq[String]] = plainText.mapPartitions(iter => {
        val pipeline = createNLPPieline()
        iter.map { case (title, contents) => (title, plainTextToLemmas(contents, stopWords, pipeline)) }
      }
      )

      val filtered = lemmatized.filter(_._2.size > 1)

      documentTermMatrix(filtered, stopWords, numTerms, sc)
    }

    /** 词行合并
      *
      * @StanfordCoreNLP pipline
      */
    def createNLPPieline(): StanfordCoreNLP = {
      val props = new Properties()
      props.put("annotators", "tokenize,ssplit,pos,lemma")
      new StanfordCoreNLP(props)
    }

    /**
      * 判断是否为字母
      *
      * @param str
      * @return
      */
    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }


    def plainTextToLemmas(text: String, stopWords: Set[String], pipline: StanfordCoreNLP)
    : Seq[String] = {
      val doc = new Annotation(text)
      pipline.annotate(doc)

      val lemmas = new ArrayBuffer[String]()
      val sentences = doc.get(classOf[SentencesAnnotation])
      for (sentence <- sentences;
           token <- sentence.get(classOf[TokensAnnotation])) {
        val lemma = token.get(classOf[LemmaAnnotation])
        if (lemma.length > 2 && !stopWords.contains(lemma)
          && isOnlyLetters(lemma))
        )
        {
          lemmas += lemma.toLowerCase
        }

      }
      lemmas
    }


    /**
      * 将路径下的文件按行转化成一个set集合
      *
      * @param path
      * @return
      */
    def loadStopWords(path: String) = scala.io.Source.fromFile(path).getLines.toSet

    def readFile(path: String, sc: SparkContext): RDD[String] = {
      val conf = new Configuration()
      val rawXmls = sc.parallelize("page.word")
      rawXmls.map(p => p.toString)
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
