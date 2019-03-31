package jp.soonraah.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.rand
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class CustomMatchersSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  private val spark: SparkSession = SparkSession
    .builder
    .appName("CustomMatchersSpec")
    .master("local[*]")
    .getOrCreate()

  import CustomMatchers._

  override def afterAll(): Unit = {
    spark.stop
  }

  describe("haveTheSameContentsAs()") {
    import spark.implicits._

    describe("when Dataset contents and expected values are same") {
      it("should make a success of test") {
        val df = Seq((1, "foo"), (2, "bar")).toDF("id", "name")
        val expected = Seq(Row(1, "foo"), Row(2, "bar"))

        df should haveTheSameContentsAs(expected)
      }
    }

    describe("when Dataset contents and expected values are different") {
      it("should make a failure of test") {
        val df = Seq((1, "foo"), (9, "BAZ")).toDF("id", "name")
        val expected = Seq(Row(1, "foo"), Row(2, "bar"))

        the [TestFailedException] thrownBy {
          df should haveTheSameContentsAs(expected)
        } should have message "Dataset(WrappedArray([1,foo], [9,BAZ])) did not have same contents as List([1,foo], [2,bar])"
      }
    }

    describe("when shuffled Dataset and expected values are given") {
      it("should ignore the order") {
        val data = (0 until 100).map { i => (i, s"name$i") }

        // shuffle both in different way
        val df = data.toDF("id", "name").orderBy(rand())
        val expected = scala.util.Random.shuffle(data).map { d => Row(d._1, d._2) }

        df should haveTheSameContentsAs(expected)
      }
    }
  }
}
