package study

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by apple on 17/5/19.
  */
object MllibHomeWork1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "lession1", new SparkConf())

    val data = sc.textFile("/Users/apple/Documents/inputPath/SparkMLib/homework1/")
    //1.统计数据总行数，2.用户数量，3.日期有哪几天
    //1
    println(data.count())

    //2
    //val userNum=data.flatMap(_.split("  ")).reduce((x,y) => y)
    val userNum = data.flatMap(_.split("\t")(1)).distinct().count()
    println(userNum)
    //返回的是每一个split后的元素
    val userNum1 = data.flatMap(_.split("\t")(1)).distinct().count()

    //3
    val dateNum = data.flatMap(_.split("\t")(0)).distinct()
    dateNum.foreach(print(_))

    //4.指定任意连续两天，计算每个用户第二天的新安装包名
    val installName = data.filter(_.split("\t")(0) == "2016-03-30").map(_.split("\t")(2))
    val result = data.map(_.split("\t")(0)).intersection(installName)
    println("-------------4-----------------")
    result.foreach(println(_))

    //任意指定一天数据，根据每个用户的安装列表，统计每个包名的安装用户数据数量，从小到大排序。
    //并去前1000个包名，最后计算这1000个包名之间的坚持度和置信度
    val oneDay = data.filter(_.split("\t")(0) == "2016-03-30")
    val installList=oneDay.map{
      line => (line.split("\t")(2),line.split("\t")(1))
    }
    println("-------------5-----------------")
    val statistica = installList.countByKey()
    statistica.foreach(println(_))
  }
}