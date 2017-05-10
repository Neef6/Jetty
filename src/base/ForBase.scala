package base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by apple on 17/5/9.
  */
object ForBase {
  val conf = new SparkConf()
  val sc = new SparkContext("local","wordcount",conf)


  val data = Array(2,4,5,6,7)
  //product data
  val distData = sc.parallelize(data,3) // 3 3个partition

  distData.collect();
  distData.take(1);

  //read 本地，hdfs，压缩，当前目录，多个文件，可用通配符
  val distFile= sc.textFile("data.txt");

  //处理
  //map filter
  // flatMap 1对多， 可以输入1个输出多个
  // mapPartitions  对分区进行操作，输入的是每个分区
  //sample 抽样
  val a = sc.parallelize(1 to 100000,3)
  a.sample(false,0.1,0).count
  //union
  //intersection 交集
  //distinct
 //groupbykey,reduce

  //聚合  aggregateByKey
  val z =sc.parallelize(List(1,2,3,4,5,6))
  //参数 初始值 seq操作 comb操作 0表示初始值
  z.aggregate(0)(math.max(_,_),_ + _) //0和1比，大值和2比    9

  val z1=sc.parallelize(List((1,3),(1,2),(1,4),(2,3)))
  z1.aggregateByKey(0)(math.max(_,_),_+_)

  //combineByKey(createCombiner,mergeValue.mergeCombiners)
  val data1 = Array((1,1.0),(1,2.0),(1,3.0),(1,4.0),(1,5.0))
  val rdd= sc.parallelize(data1,2)
  val combine1=rdd.combineByKey(createCombiner = ())



}
