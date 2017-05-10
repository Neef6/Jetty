package base

/**
  * Created by apple on 17/3/9.
  */
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext("local","wordcount",conf)
    val line = sc.textFile("/Users/apple/Documents/inputPath/word.txt")
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
    sc.stop()
  }
}
