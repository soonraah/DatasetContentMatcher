package jp.soonraah.spark

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

class CustomMatchers {
  /**
    * A custom matcher to verify that a given Dataset have the same contents as an expected sequence.
    *
    * @param expectedContents A sequence with expected contents
    * @tparam T A type of Dataset content
    */
  class DatasetContentMatcher[T](expectedContents: Seq[T]) extends Matcher[Dataset[T]] {
    override def apply(left: Dataset[T]): MatchResult = {
      val collected = left.collect.toSeq.sortBy(_.toString)
      val expected = expectedContents.sortBy(_.toString)

      MatchResult(
        collected == expected,
        s"Dataset($collected) did not have same contents as $expected",
        s"Dataset($collected) have same contents as $expected"
      )
    }
  }

  /**
    * This method enables the following syntax:
    *
    * <pre class="stHighlight">
    * ds should haveTheSameContentsAs (Seq(Row(1, "foo"), Row(2, "bar")))
    *           ^
    * </pre>
    */
  def haveTheSameContentsAs[T](expected: Seq[T]) = new DatasetContentMatcher(expected)
}

object CustomMatchers extends CustomMatchers
