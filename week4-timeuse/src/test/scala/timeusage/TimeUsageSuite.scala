package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSpec}

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSpec with BeforeAndAfterAll {

  val uut = TimeUsage

  import uut.spark.implicits._

  describe("read") {
    it("should load a csv file") {
      val (columns, initDf) = uut.read("/timeusage/atussum-fixture.csv")
      //println(s"cols: $columns") initDf.show()
      initDf.count() should equal(10)
    }

    it("should load a schema from cols names list") {
      val columnNames = List("a", "b", "c")

      dofSchemaAsserts(columnNames, uut.dfSchema(columnNames))
      dofSchemaAsserts(columnNames, uut.dfSchemaViaGenerator(columnNames))
    }

    it("should map a line to a Row") {
      val line1 = "\"20030100013280\",1,-1,44,2,2,60,2,2,-1,-1,1,2,0,2,66000,0,-1,1,-1,6,8155463,30,2003,870,0,0,40,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,325,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,200,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
      val row = uut.row(line1.split(",").to[List])
      assert(row.get(0) == "\"20030100013280\"")
      assert(row.get(1) == 1.0)
      assert(row.get(2) == -1.0)
      assert(row.get(3) == 44.0)
    }

    def dofSchemaAsserts(columnNames: List[String], st: StructType) = {
      val checkField =
        (index: Int, expType: DataType) => {
          assert(st.fields(index).dataType == expType)
          assert(!st.fields(index).nullable)
        }

      assert(st.fields.length == columnNames.length)
      checkField(0, DataTypes.StringType)

      for (i <- 1 until columnNames.length)
        checkField(i, DataTypes.DoubleType)
    }
  }

  describe("misc test") {
    it("dummy testDatasetGroupAndAggregate") {
      testDatasetGroupAndAggregate() should equal(Array(
        (1, "thisisa"),
        (3, "messag"),
        (2, "e")
      ))
    }


    def testDatasetGroupAndAggregate(): Array[(Int, String)] = {
      val ds = List((3, "me"), (1, "thi"), (2, "e"), (3, "ssag"), (1, "sisa")).toDS()

      // Dataset: Grouping + aggregating, several ways
      // ex with dataset ds: group by key, concat strings
      // val v = ds.groupByKey(_._1)
      // v.mapValues(_._2).reduceGroups((acc, s) => acc + s)
      // * inefficient: groupByKey+mapGroups:
      // v.mapGroups((k, iv) => (k, iv.map(_._2).reduce(_ + _))).show() // scala : map+reduce
      // v.mapGroups((k, ikv) => (k, ikv.foldLeft("")((acc, e) => acc + e._2))) // scala : foldLeft

      // Using Aggregator instead:
      val myAgg = new Aggregator[(Int, String), String, String] {

        override def zero: String = ""

        override def reduce(acc: String, p: (Int, String)): String = acc + p._2

        override def merge(b1: String, b2: String): String = b1 + b2

        override def finish(reduction: String): String = reduction

        override def bufferEncoder: Encoder[String] = Encoders.STRING

        override def outputEncoder: Encoder[String] = Encoders.STRING
      }.toColumn

      val x = ds.groupByKey(_._1).agg(myAgg)
      //x.show()
      x.collect()
    }
  }
}
