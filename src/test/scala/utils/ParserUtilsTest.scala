package utils

import com.stratio.utils.ParserUtils
import org.scalatest._

class ParserUtilsTest extends FlatSpec with ShouldMatchers {

  "ParserUtils" should "parse proper dates" in {
    val stringDate = "2014-03-02"
    ParserUtils.parseDate(stringDate) should be(None)
  }

  it should "not parse wrong dates" in {
    val stringDate = "03-2014-78"
    ParserUtils.parseDate(stringDate) should be(Some(ParserUtils.NOT_PARSEABLE_DATE_ERROR))
  }
}
