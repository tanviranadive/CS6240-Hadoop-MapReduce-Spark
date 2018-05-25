import org.apache.spark._
import org.apache.spark.SparkConf

object PageRank {

  def main(args: Array[String]): Unit = {

    // create a spark context
    val sc = new SparkContext(new SparkConf()
      .setAppName("Page Rank")
      .setMaster("local"))

    val fileName = args(0)
    val outFile = args(1)

    // Read and parse the input using mapPartitions for faster processing
    // Apply reduceByKey to handle duplicates if there are any
    // Gives RDD of the format : (PageName, outlinks)

    val parsedData = sc.textFile(fileName)
      .mapPartitions(rows => parseRows(rows))
      .map(getPageAndLinks)
      .reduceByKey((v1,v2) => concatValues(v1,v2))
      .cache()


    // Calculate total pages and initial pageRank and assign the initial page rank to pages

    val pageCount = parsedData.count
    val initialPageRank = 1.0/pageCount


    // Gives graph of the form (PageName, (PageRank, outlinks))

    var pageRankGraph = parsedData.map(row => (row._1, (initialPageRank, row._2)))

    val iterations = 10

    // 10 iterations
    for(i <- 0 to iterations){

      // Calculate sink sum for each iteration
      val sinkSum = pageRankGraph
        .filter{case(pageName, (pageRank, outlinks)) => outlinks(0)== ""}
        .map(row => row._2._1)
        .sum()


      // Calculate new Page Rank, assign pageRank contribution of each inlink to page
      // flatMap gives a single list of (outlinkPageName, PageRank)
      // reduceBYKey on this will give contributions of all inlinks to outlinkPage

      val newPageData = pageRankGraph
        .filter{case(pageName, (pageRank,outlinks)) => (outlinks.size>1 || (outlinks.size==1 &&  outlinks(0) != ""))}
        .flatMap{case(pageName,(pageRank,outlinks)) => outlinks
          .map(outlink=>(outlink.trim(),pageRank/outlinks.size))}
        .reduceByKey((sumPR, pageRank) => sumPR + pageRank)
        .map(row => (row._1,calcNewPageRank(row._2,sinkSum,pageCount)))


      // Use join to join old page graph and update the page rank to new pageRank calculated in previous step
      pageRankGraph = newPageData.join(parsedData)

    }

    // Sort pages on PageRank in descending order using takeOrdered funciton

    val top100Pages = pageRankGraph.map(row=>(row._1,row._2._1))
      .takeOrdered(100)(Ordering[Double].reverse.on(row=>row._2))


    // Use repartition and sorting to get single text file as output

    val sortedPages = sc.parallelize(top100Pages)
      .map(row => (row._2 * (-1), row._1))
      .cache()

    val topKPages = sortedPages.repartitionAndSortWithinPartitions(new RangePartitioner(1,sortedPages))
      .map(row => (row._2, row._1*(-1)))


    // Save to text file

    topKPages.saveAsTextFile(outFile)

    sc.stop()
  }

  // function for parsing, calls BZ2Parser
  def parseRows(pages: Iterator[String]): Iterator[String] = {
    val bz: Bz2WikiParser = new Bz2WikiParser()
    pages.map(page => bz.parse(page))
  }


  // Output of the format (pageName, outlinks) as Pair RDD
  def getPageAndLinks(parsedPage: String): (String, Array[String])={

    val parts = parsedPage.split(" -> ")

    val pageName = parts(0).trim

    var outlinks: Array[String] = null

    if(parts.length == 2){
      outlinks = parts(1).trim().split(" , ")
    } else {
      outlinks = Array("")
    }

    (pageName, outlinks)
  }


  // Calculate the new Page Rank
  def calcNewPageRank(value: Double, sinkSum: Double, pageCount: Long): (Double) ={
    val alpha = 0.15f
    ((alpha/pageCount) + ((1-alpha)*value) + ((1-alpha)*sinkSum/pageCount))

  }

  // Get rid of dupliactes
  def concatValues(strings: Array[String], strings1: Array[String]): Array[String] ={
    if(strings.length>=1 && strings(0)!=""){
      (strings)
    }

    else (strings1)
  }

}