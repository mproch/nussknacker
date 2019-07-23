package pl.touk.http.argonaut

import argonaut.{Argonaut, JsonParser}
import org.scalatest.{FunSuite, Matchers}

class JsonMarshallerTest extends FunSuite with Matchers {

  test("should handle utf properly") {

    val argonautJson = Argonaut.jString("łódź, gżegżółka, конечно мы знаем УТФ ")

    val bytes = JacksonJsonMarshaller.marshall(argonautJson)
    val string = JacksonJsonMarshaller.marshallToString(argonautJson)

    JsonParser.parse(string) shouldBe Right(argonautJson)
    JsonParser.parse(new String(bytes)) shouldBe Right(argonautJson)


  }

}
