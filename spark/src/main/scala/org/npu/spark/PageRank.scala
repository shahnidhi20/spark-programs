package org.npu.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object PageRank {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
|Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    // val iters = 3
    val iters = if (args.length > 0) args(1).toInt else 10
    println("No of iterations: " +  iters)
    val ctx = new SparkContext(sparkConf)

    val lines = ctx.textFile(args(0), 1)

    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

      val output1 = ranks.collect()

      output1.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    }

     ranks.saveAsTextFile("pr.output.txt")

    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    ctx.stop()

  }

}