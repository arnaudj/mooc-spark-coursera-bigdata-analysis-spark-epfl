package wikipedia

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  def initializeWikipediaRanking(): Boolean =
    try {
      WikipediaRanking
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    sc.stop()
  }

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def assertEquivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Unit = {
    // (1)
    assert(given.toSet == expected.toSet, "The given elements are not the same as the expected elements")
    // (2)
    assert(
      !(given zip given.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 }),
      "The given elements are not in descending order"
    )
  }

  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    assert(occurrencesOfLang("Java", rdd) == 1, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
    assert(occurrencesOfLang("C++", rdd) == 0)
  }

  test("'occurrencesOfLang' should work for (specific) RDD with 4 elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(
      WikipediaArticle("title1", "Java Jakarta"),
      WikipediaArticle("title2", "Java Abc"),
      WikipediaArticle("title3", "C++ bef"),
      WikipediaArticle("title4", "Pascal ghi")
    ))
    assert(occurrencesOfLang("Java", rdd) == 2, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
    assert(occurrencesOfLang("C++", rdd) == 1)
    assert(occurrencesOfLang("Pascal", rdd) == 1)
    assert(occurrencesOfLang("ASM", rdd) == 0)
  }

  test("'rankLangs' should work for RDD with two elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    val res = ranked.head._1 == "Scala"
    assert(res)
  }

  test("'rankLangs' should work for RDD with 5 elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "PHP", "MATLAB", "Ruby", "C++")
    val rdd = sc.parallelize(List(
      WikipediaArticle("1", "Scala is great"),
      WikipediaArticle("2", "Really Scala is great"),
      WikipediaArticle("3", "Java is OK, but Scala is cooler"),
      WikipediaArticle("4", "PHP is OK, but Fake is cooler"),
      WikipediaArticle("5", "C++ is OK, but PHP is cooler")
    ))
    val ranked = rankLangs(langs, rdd)
    assert(ranked == List(("Scala", 3), ("PHP", 2), ("Java", 1), ("C++", 1), ("MATLAB", 0), ("Ruby", 0)))
  }

  test("'makeIndex' creates a simple index with two entries") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val a1 = WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang")
    val a2 = WikipediaArticle("2","Scala and Java run on the JVM")
    val a3 = WikipediaArticle("3","Scala is not purely functional")

    val articles = List(
        a1,
        a2,
        a3
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    assert(index.count() == 2)
    assert(index.map(p => (p._1, p._2.toList)).sortBy(_._1).collect() sameElements Array(
      ("Java", List(a2)),
      ("Scala", List(a2, a3))
    ))
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val a1 = WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang")
    val a2 = WikipediaArticle("2","Scala and Java run on the JVM")
    val a3 = WikipediaArticle("3","Scala is not purely functional")

    val articles = List(
      a1,
      a2,
      a3
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    assert((ranked.head._1 == "Scala"))
    assert(index.map(p => (p._1, p._2.toList)).sortBy(_._1).collect() sameElements Array(
      ("Java", List(a2)),
      ("Scala", List(a2, a3))
    ))
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java"),
        WikipediaArticle("5","Java is for enterprise developers")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    val res = (ranked.head._1 == "Java")
    assert(res)
  }


}
