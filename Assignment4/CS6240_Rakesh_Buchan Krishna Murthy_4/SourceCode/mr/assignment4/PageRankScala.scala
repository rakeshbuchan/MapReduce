package mr.assignment4

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author rakesh
 *
 */
object PageRankScala {
  def main(args: Array[String]) = {

    /* Start the Spark context */
    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("yarn")
    val sc = new SparkContext(conf)

    /* Call the Preprocessor on each line of the input file, which returns the output in the 
     * following format:-
     * pageName # outlink1~outlink2~outlink3... for valid pages with outlinks and 
     * pageName # for valid pages with no outlinks and 
     * "" for invalid pages
     */

    val input = sc.textFile(args(0))
      .map(line => Preprocessor.readLine(line))
      .filter(line => !line.equals("")) // remove invalid lines
      .map(line => line.split(" # ")) //split the line to extract pageName and it's outlinks
      .map(line => if (line.length == 1) {
        (line(0), List()) // when no outlinks are present
      } else {
        (line(0), line(1).split("~").toList) // makes a list of all outlinks
      })

    input.persist() //persisting for making the processing quick                                     

    /* Ensures that all the PageNames and outlinks in the corpus are considered */
    var allPagesWithLinks = input.values //extract all the outlinks
      /* creating a pair RDD for each outlink - outlink, list()*/
      .flatMap { link => link }
      .keyBy(link => link)
      .map(line => (line._1, List[String]()))

      .union(input) //unions it with the original input

      /* reduces the union created above by pageName and concatenating all it's outlinks */
      .reduceByKey((outlink1, outlink2) => outlink1.++(outlink2))

    val noOfPages = allPagesWithLinks.count() // gets the count of all pages

    val initialPageRank = 1.0 / noOfPages // calculates the initial page rank for all pages
    val alpha: Double = 0.15

    /*creating a pair RDD with pageName and it's page rank*/
    var pageWithPageRanks = allPagesWithLinks.keys
      .map { page => (page, initialPageRank) }

    for (i <- 1 to 10) {
      var danglingScore = sc.accumulator(0.0) // set the dangling value to 0.0 at the start for each iteration

      /* creates a pair RDD with pageName, (List(outlinks), pageRank) */
      var updatedPageRank = allPagesWithLinks
        .join(pageWithPageRanks)

        .values // extracts (List(outlinks), pageRank)
        .flatMap { //for each outlink in outlinks list          
          case (outlinks, pageRank) =>
            val outlinksCount = outlinks.size // finds the number of outlinks
            /* if it's a dangling page */
            if (outlinksCount == 0) {
              danglingScore += pageRank //updates dangling score
              List()
            } /* Else distributes the current pageRank to all it's outlinks */ else {
              outlinks.map { link => (link, pageRank / outlinksCount) }
            }
        }
        .reduceByKey(_ + _) // merges all the pageRank contributions for a particular page

      updatedPageRank.first()

      val finalDanglingScore: Double = danglingScore.value

      /* Finding the pages with no in-links and assigning their page ranks to 0.0, 
       * since they did not receive any contributions from other pages
       */
      pageWithPageRanks = pageWithPageRanks
        .subtractByKey(updatedPageRank)
        .map(pageName => (pageName._1, 0.0))
        .union(updatedPageRank)

        /* Calculating Page Ranks */
        .mapValues[Double](pageRank => alpha * initialPageRank +
          (1 - alpha) * (finalDanglingScore / noOfPages + pageRank))
    }

    /* Extract top 100 ranked pages and printing into the output file*/
    val top100Pages = sc.parallelize(pageWithPageRanks
      .map(x => (x._2, x._1))
      .top(100), 1)
      .saveAsTextFile(args(1))

  }
}