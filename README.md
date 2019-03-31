# DatasetContentMatcher

## Description

**DatasetContentMatcher** is a custom matcher for ScalaTest to check contents of Apache Spark `Dataset`.
Its goal is making it easy to test features to make or convert `Dataset` instance on ScalaTest.

## How to Use

Like this.

```scala
import jp.soonraah.spark.CustomMatchers._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class FooSpec extends FunSpec {
  describe("agePlusOne()") {
    it("should convert correctly") {
      val spark: SparkSession = ...
      import spark.implicits._
      
      val df: DataFrame = Seq(("johnny", 37), ("jimmie", 29)).toDF("name", "age")
      val sus = new Foo()
      val actual: DataFrame = sus.agePlusOne(df)
      actual should haveTheSameContentsAs(Seq(Row("jonny", 38), Row("jimmie", 30)))
    }
  }
}

```

See also [test file](https://github.com/soonraah/DatasetContentMatcher/blob/master/src/test/scala/jp/soonraah/spark/CustomMatchersSpec.scala).
