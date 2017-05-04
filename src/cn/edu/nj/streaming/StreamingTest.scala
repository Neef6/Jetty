package cn.edu.nj.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by apple on 17/4/19.
  */
object StreamingTest {
  def main(args: Array[String]): Unit = {
    //2 means the number of thread.one of the thread need to compute the time
    val sparkConf = new SparkConf().setAppName("wordcountStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val lines = ssc.textFileStream("/Users/apple/Documents/inputPath/SparkMLib/04/streamingTest/")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
