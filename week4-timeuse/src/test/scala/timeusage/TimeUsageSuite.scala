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

  describe("classifiedColumns") {
    it("should classify columns into 3 categories whose content do not intersect") {
      val prodColumns = List("tucaseid", "gemetsta", "gtmetsta", "peeduca", "pehspnon", "ptdtrace", "teage", "telfs", "temjot", "teschenr", "teschlvl", "tesex", "tespempnot", "trchildnum", "trdpftpt", "trernwa", "trholiday", "trspftpt", "trsppres", "tryhhchild", "tudiaryday", "tufnwgtp", "tehruslt", "tuyear", "t010101", "t010102", "t010199", "t010201", "t010299", "t010301", "t010399", "t010401", "t010499", "t010501", "t010599", "t019999", "t020101", "t020102", "t020103", "t020104", "t020199", "t020201", "t020202", "t020203", "t020299", "t020301", "t020302", "t020303", "t020399", "t020401", "t020402", "t020499", "t020501", "t020502", "t020599", "t020681", "t020699", "t020701", "t020799", "t020801", "t020899", "t020901", "t020902", "t020903", "t020904", "t020905", "t020999", "t029999", "t030101", "t030102", "t030103", "t030104", "t030105", "t030108", "t030109", "t030110", "t030111", "t030112", "t030186", "t030199", "t030201", "t030202", "t030203", "t030204", "t030299", "t030301", "t030302", "t030303", "t030399", "t030401", "t030402", "t030403", "t030404", "t030405", "t030499", "t030501", "t030502", "t030503", "t030504", "t030599", "t039999", "t040101", "t040102", "t040103", "t040104", "t040105", "t040108", "t040109", "t040110", "t040111", "t040112", "t040186", "t040199", "t040201", "t040202", "t040203", "t040204", "t040299", "t040301", "t040302", "t040303", "t040399", "t040401", "t040402", "t040403", "t040404", "t040405", "t040499", "t040501", "t040502", "t040503", "t040504", "t040505", "t040506", "t040507", "t040508", "t040599", "t049999", "t050101", "t050102", "t050103", "t050189", "t050201", "t050202", "t050203", "t050204", "t050289", "t050301", "t050302", "t050303", "t050304", "t050389", "t050403", "t050404", "t050405", "t050481", "t050499", "t059999", "t060101", "t060102", "t060103", "t060104", "t060199", "t060201", "t060202", "t060203", "t060289", "t060301", "t060302", "t060303", "t060399", "t060401", "t060402", "t060403", "t060499", "t069999", "t070101", "t070102", "t070103", "t070104", "t070105", "t070199", "t070201", "t070299", "t070301", "t070399", "t079999", "t080101", "t080102", "t080199", "t080201", "t080202", "t080203", "t080299", "t080301", "t080302", "t080399", "t080401", "t080402", "t080403", "t080499", "t080501", "t080502", "t080599", "t080601", "t080602", "t080699", "t080701", "t080702", "t080799", "t080801", "t080899", "t089999", "t090101", "t090102", "t090103", "t090104", "t090199", "t090201", "t090202", "t090299", "t090301", "t090302", "t090399", "t090401", "t090402", "t090499", "t090501", "t090502", "t090599", "t099999", "t100101", "t100102", "t100103", "t100199", "t100201", "t100299", "t100381", "t100383", "t100399", "t100401", "t100499", "t109999", "t110101", "t110199", "t110281", "t110289", "t119999", "t120101", "t120199", "t120201", "t120202", "t120299", "t120301", "t120302", "t120303", "t120304", "t120305", "t120306", "t120307", "t120308", "t120309", "t120310", "t120311", "t120312", "t120313", "t120399", "t120401", "t120402", "t120403", "t120404", "t120405", "t120499", "t120501", "t120502", "t120503", "t120504", "t120599", "t129999", "t130101", "t130102", "t130103", "t130104", "t130105", "t130106", "t130107", "t130108", "t130109", "t130110", "t130111", "t130112", "t130113", "t130114", "t130115", "t130116", "t130117", "t130118", "t130119", "t130120", "t130121", "t130122", "t130123", "t130124", "t130125", "t130126", "t130127", "t130128", "t130129", "t130130", "t130131", "t130132", "t130133", "t130134", "t130135", "t130136", "t130199", "t130201", "t130202", "t130203", "t130204", "t130205", "t130206", "t130207", "t130208", "t130209", "t130210", "t130211", "t130212", "t130213", "t130214", "t130215", "t130216", "t130217", "t130218", "t130219", "t130220", "t130221", "t130222", "t130223", "t130224", "t130225", "t130226", "t130227", "t130228", "t130229", "t130230", "t130231", "t130232", "t130299", "t130301", "t130302", "t130399", "t130401", "t130402", "t130499", "t139999", "t140101", "t140102", "t140103", "t140104", "t140105", "t149999", "t150101", "t150102", "t150103", "t150104", "t150105", "t150106", "t150199", "t150201", "t150202", "t150203", "t150204", "t150299", "t150301", "t150302", "t150399", "t150401", "t150402", "t150499", "t150501", "t150599", "t150601", "t150602", "t150699", "t159989", "t160101", "t160102", "t160103", "t160104", "t160105", "t160106", "t160107", "t160108", "t169989", "t180101", "t180199", "t180280", "t180381", "t180382", "t180399", "t180481", "t180482", "t180499", "t180501", "t180502", "t180589", "t180601", "t180682", "t180699", "t180701", "t180782", "t180801", "t180802", "t180803", "t180804", "t180805", "t180806", "t180807", "t180899", "t180901", "t180902", "t180903", "t180904", "t180905", "t180999", "t181002", "t181081", "t181099", "t181101", "t181199", "t181201", "t181202", "t181204", "t181283", "t181299", "t181301", "t181302", "t181399", "t181401", "t181499", "t181501", "t181599", "t181601", "t181699", "t181801", "t181899", "t189999", "t500101", "t500103", "t500104", "t500105", "t500106", "t500107", "t509989")
      val (primaryNeeds, workingActivities, otherActivities) = uut.classifiedColumns(prodColumns)

      //println(s"Got prodColumns: ${prodColumns}") println(s"Got primaryNeeds: ${primaryNeeds}") println(s"Got workingActivities: ${workingActivities}") println(s"Got otherActivities: ${otherActivities}")

      prodColumns.length shouldBe 455
      primaryNeeds.length shouldBe 55
      workingActivities.length shouldBe 23
      otherActivities.length shouldBe 346

      primaryNeeds.intersect(workingActivities).length shouldBe 0
      primaryNeeds.intersect(otherActivities).length shouldBe 0
      workingActivities.intersect(otherActivities).length shouldBe 0
    }
  }

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
