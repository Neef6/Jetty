package cn.edu.nj

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by apple on 17/3/29.
  */
object MusicRecommend {

  /**
    * user_artist_data.txt
    * 3 columns: userid artistid playcount
    *
    * artist_data.txt
    * 2 columns: artistid artist_name
    *
    * artist_alias.txt
    * 2 columns: badid, goodid
    *
    */

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("recommend"))
    val base = "/Users/apple/Documents/inputPath/Spark/musicrecommend"
    val userArtistData = sc.textFile(base + "user_artist_data.txt")
    var artistAlias = sc.textFile(base + "artist_alias.txt")
    var artistData = sc.textFile(base + "artist_data.txt")

    partition(userArtistData, artistAlias, artistData)
    model(sc, userArtistData, artistData, artistAlias)
  }


  def buildArtistByID(artistData: RDD[String]) =
    artistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }


  def buildArtistAlias(artistAlias: RDD[String]): Map[Int, Int] =
    artistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }

    }.collectAsMap()

  def partition(userArtistData: RDD[String], artistAlias: RDD[String], artistData: RDD[String]) = {

    val userId = userArtistData.map(_.split(' ')(0).toDouble).stats()
    val itermId = userArtistData.map(_.split(' ')(1).toDouble).stats()

    val artistId = buildArtistByID(artistData)
    val artistAlias = buildArtistAlias(artistAlias)

    val (badId, goodId) = artistAlias.head
  }

  def model(sc: SparkContext, userArtistData: RDD[String], artistData: RDD[String], artistAlias: RDD[String]) =

}
