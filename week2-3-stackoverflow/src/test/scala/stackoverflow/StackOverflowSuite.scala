package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120

    def getPostingsCount(raw: RDD[Posting]) = {
      val count = raw.count() // trigger action
      assert(count > 0, "Expected some posts to be loaded")
      count
    }
  }

  /**
    * Question -> answers (for answers.count >= 1)
    * 5484340- > 5494879
    * 9002525 -> 9003401, 9003942, 9005311
    * 21984912 -> 21985273
    * 28903923 -> 28904080
    */
  private val testLines: List[String] =
    """1,27233496,,,0,C#
      |1,23698767,,,9,C#
      |1,5484340,,,0,C#
      |2,5494879,,5484340,1,
      |1,9419744,,,2,Objective-C
      |1,26875732,,,1,C#
      |1,9002525,,,2,C++
      |2,9003401,,9002525,4,
      |2,9003942,,9002525,1,
      |2,9005311,,9002525,0,
      |1,5257894,,,1,Java
      |1,21984912,,,0,Java
      |2,21985273,,21984912,0,
      |1,27398936,,,0,PHP
      |1,28903923,,,0,PHP
      |2,28904080,,28903923,0,
      |1,20990204,,,6,PHP
      |1,5077978,,,-2,Python""".stripMargin.split("\r?\n").filter(_ != "").toList

  override def beforeAll(): Unit = {
    assert(initializeStackOverflow() != None)
  }

  override def afterAll(): Unit = {
    assert(initializeStackOverflow() != None)
    StackOverflow.sc.stop()
  }

  def initializeStackOverflow(): AnyRef =
    try {
      testObject
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        None
    }

  test("testObject can be instantiated") {
    assert(initializeStackOverflow() != None, "Can't instantiate a StackOverflow object")
  }

  test("RDD[Posting] can be extracted") {
    assert(initializeStackOverflow() != None, "Can't instantiate a StackOverflow object")
    createRawPostings()
  }

  private def createRawPostings(): RDD[Posting] = {
    val lines: RDD[String] = StackOverflow.sc.parallelize(testLines)
    val raw: RDD[Posting] = testObject.rawPostings(lines)
    raw.cache()
    val postingsCount = testObject.getPostingsCount(raw) // action
    assert(postingsCount == 18, "Invalid number of extracted postings")
    assert(postingsCount == testLines.size)
    raw
  }

  test("RDD[Posting] can be extracted then grouped") {
    assert(initializeStackOverflow() != None, "Can't instantiate a StackOverflow object")
    val raw: RDD[Posting] = createRawPostings()

    val grouped: RDD[(Int, Iterable[(Posting, Posting)])] = testObject.groupedPostings(raw)

    val res = grouped.collect.toList
    assert(res.size == 4)
    /*  question, 1 answer
      |1,5484340,,,0,C#
      |2,5494879,,5484340,1,
     */
    assert(res(0)._1 == 5484340)
    assert(res(0)._2.size == 1)
    val postings0 = res(0)._2.toList
    assert(postings0(0)._1.id == 5484340) // question
    assert(postings0(0)._2.id == 5494879) // answer

    /* question, 3 answers
      |1,9002525,,,2,C++
      |2,9003401,,9002525,4,
      |2,9003942,,9002525,1,
      |2,9005311,,9002525,0,
     */
    assert(res(1)._1 == 9002525)
    assert(res(1)._2.size == 3)
    val postings1 = res(1)._2.toList
    assert(postings1(0)._1.id == 9002525) // question
    assert(postings1(0)._2.id == 9003401) // answer
    assert(postings1(1)._1.id == 9002525) // question
    assert(postings1(1)._2.id == 9003942) // answer
    assert(postings1(2)._1.id == 9002525) // question
    assert(postings1(2)._2.id == 9005311) // answer
  }
}
