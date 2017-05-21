package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.io.StdIn
import scala.runtime.ScalaRunTime

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable {
  /**
    * <postTypeId>:     Type of the post. Type 1 = question, type 2 = answer.
    * <id>:             Post UUID
    * <acceptedAnswer>: Id of the accepted answer post (optional (empty string))
    * <parentId>:       For an answer: id of the corresponding question. For a question: missing (empty string)
    * <score>:          The StackOverflow score (based on user votes).
    * <tag>:            Question: programming language tag. Answer: empty string
    *
    * Layout:
    * 1 Question has N answers (1 is acceptedAnswer, if any) (based *.parentId = question.id)
    */
  def isQuestion: Boolean = postingType == 1

  def isAnswer: Boolean = postingType == 2

  def pseudoType: String = if (isQuestion) "Question" else if (isAnswer) "Answer" else "?Posting?"

  override def toString: String = ScalaRunTime._toString(this).replace("Posting", pseudoType)
}


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val MAIN_ASSERT_STEPS = false
  val MAIN_DEVMODE = false

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    if (MAIN_ASSERT_STEPS) {
      assert(raw.count() == 8143801, "Invalid number of extracted postings")
    }

    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    if (MAIN_ASSERT_STEPS) {
      val scoredCount = scored.count()
      assert(scoredCount == 2121822, s"Invalid number of extracted scores: ${scoredCount}")
    }

    val vectors: RDD[(DataPoint)] = vectorPostings(scored)
    if (MAIN_ASSERT_STEPS) {
      val vectorsCount = vectors.count()
      assert(vectorsCount == 2121822, s"Invalid number of extracted vectors: ${vectorsCount}")
      assert(vectors.filter(p => p._1 == 350000 && p._2 == 67).count() >= 1)
      assert(vectors.filter(p => p._1 == 100000 && p._2 == 89).count() >= 1)
      assert(vectors.filter(p => p._1 == 300000 && p._2 == 3).count() >= 1)
      assert(vectors.filter(p => p._1 == 200000 && p._2 == 20).count() >= 1)
    }

    if (MAIN_DEVMODE) { // no cache/persist for coursera grader, else OOM
      vectors.persist(StorageLevel.DISK_ONLY)
    }

    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

    if (MAIN_DEVMODE) {
      println("All good! Press any key to quit")
      StdIn.readChar()
    }
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[( /* Question:*/ Posting, /* Answer: */ Posting)])] = {
    val answers: RDD[(Int, Posting)] = postings.filter(_.isAnswer).map(p => (p.parentId.getOrElse(-1), p))
    val questions: RDD[(Int, Posting)] = postings.filter(_.isQuestion).map(p => (p.id, p))
    // we drop orphan questions (with no answer, since groupedPostings ret signature doesn't allow us to do a leftOuterJoin, that would lead to pairs of (question, Option[answer]))
    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[( /* Question: */ Posting, /* MaxAnswerScore: */ Int)] = {
    val answerHighScore = (as: Array[Posting]) => { // answerHighScore RFTed as function, to avoid serializing (this<=>StackOverflow, which in test lies in StackOverflowSuite, which isn't serializable)
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.map(entry => {
      val answers: Array[Posting] = entry._2.map(_._2).toArray
      val hs: Int = answerHighScore(answers)
      val question: Posting = entry._2.head._1
      (question, hs)
    })
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[( /* Question: */ Posting, /* AnswerMaxScore: */ Int)]) : RDD[( /* langsListIndex*langSpread: */ Int, /* AnswerMaxScore: */ Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    scored.map(p => (firstLangInTag(p._1.tags, langs).getOrElse(-1) * langSpread, p._2))
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //
  type CentroidIndex = Int
  type DataPoint = (Int, Int)

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone()

    // = K-means clustering (with k centroids & clusters) on n points:
    // Init:
    // let input points : K = X1..Xn (@vectors)
    // place centroids randomly: C1..Ck (@means - done by reservoir sampling in sampleVectors)
    //
    // repeat until convergence:
    // for each point Xi:
    // - find nearest centroid Cj (min(Xi, Cj)) : assign Xi to cluster j
    // for each cluster: j = 1..k
    // - Cj changed to average of all points Xi assigned in previous step
    val points: RDD[DataPoint] = vectors
    val centroidIdToDataPoint: RDD[(CentroidIndex, DataPoint)] = points.map(point => (findClosest(point, means), point))
    val centroidIdToDataPoints: RDD[(CentroidIndex, Iterable[DataPoint])] = centroidIdToDataPoint.groupByKey()

    val centroidIdToNewCentroidDataPoint: RDD[(CentroidIndex, DataPoint)] = centroidIdToDataPoints.mapValues(centroidDataPoints => averageVectors(centroidDataPoints))
    centroidIdToNewCentroidDataPoint
      .collect() // collect to refresh newMeans locally on driver
      .foreach(e => newMeans(e._1) = e._2) // update only available centroids (some might be with 0 associated points)

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest center index */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest:RDD[(CentroidIndex, DataPoint)] = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped:RDD[(CentroidIndex, Iterable[DataPoint])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs: Iterable[/* langFactor, answerHighScore */ DataPoint] =>
      // most common language in the cluster
      val predominentLangIndex:Int = vs.map(p => (p._1 / langSpread, 1)) // (lang1, 1), (lang1, 2), (lang2, 1)...
        .groupBy(_._1)   // (lang1, (lang1, (1,1,1))), (lang2, (lang2, (1)) )  (groupBy retains _1 in aggregate)
        .mapValues(list => list.map(_._2).sum) // (lang1, 3), (lang2, 1)
        .maxBy(_._2)._1
      assert(predominentLangIndex >= 0 && predominentLangIndex < langs.size, s"Invalid predominentLangIndex: ${predominentLangIndex}")

      val langLabel: String = langs(predominentLangIndex)
      val clusterSize: Int = vs.size
      val langCount: Int = vs.count(p => (p._1 / langSpread) == predominentLangIndex)
      // percent of the questions in the most common language
      val langPercent: Double = (langCount.toDouble / clusterSize) * 100

      def computeMedian(s: Seq[Int]) = { // https://rosettacode.org/wiki/Averages/Median#Scala
        val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
        if (s.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
      }

      val arr = vs.map(_._2).toArray
      val medianScore: Int = computeMedian(arr)

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
