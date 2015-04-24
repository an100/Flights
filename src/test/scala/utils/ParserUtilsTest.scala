package utils

import com.stratio.utils.{FuelPrice, ParserUtils}
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

  it should "parse proper price list elements" in {
    val price = "1987,10,0.25"
    ParserUtils.parsePrice(price) should be(Some(FuelPrice(1987, 10, 0.25f)))
  }

  it should "not parse wrong price list elements" in {
    val price = "hello,bye,0.25"
    ParserUtils.parsePrice(price) should be(None)
  }
}
