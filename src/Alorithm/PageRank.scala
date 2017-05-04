package Alorithm

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by apple on 17/5/2.
  */
object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "pageRank", conf)
   //Array[(String, List[String])] = Array((A,List(B, C)), (B,List(A, C)), (C,List(A, B, D)), (D,List(C)))
    val links = sc.parallelize(List(("A", List("B", "C")),
      ("B", List("A", "C")), ("C", List("A", "B", "D")), ("D", List("C")))).
      partitionBy(new HashPartitioner(100)).persist()
    //cache 调用persist，cache只有一种级别，persist有好几种可选级别

    //Array[(String, Double)] = Array((A,1.0), (B,1.0), (C,1.0), (D,1.0))
    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      //join Array((A,(List(B,C),1)),(B,(List(A,C),1)....)
      //Array[(String, Double)] = Array((A,0.5), (A,0.3333333333333333), (B,0.5), (B,0.3333333333333333), (C,0.5), (C,0.5), (C,1.0), (D,0.3333333333333333))
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      //Array[(String, Double)] = Array((A,0.8583333333333333), (B,0.8583333333333333), (C,1.8499999999999999), (D,0.43333333333333335))
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)

    }
    ranks.saveAsTextFile("ranks")
  }
}
