import java.time.ZonedDateTime

//long is serialized to string...
val startDate = ZonedDateTime.now().minusHours(3).toInstant.toEpochMilli

def fields(idx: Double) = ujson.Obj(
  "recordPrefix" -> "P1",
  "recordType" -> "RT",
  "startDateTime" -> (idx + startDate),
  "amount" -> 10,
  "account" -> s"acc${idx % 101}",
  "account2" -> s"acc${idx % 103}",
  "channel" -> "POS",
  "description" -> "test11"
)

val dynamic = ujson.Obj("dummy" -> "",
  (1 to 27).map[(String, ujson.Value)](k => s"field$k" -> ujson.Obj("string" -> "value")).toArray: _*)


(1 to 10 * 1000 * 1000).foreach { idx =>
  println(ujson.Obj(fields(idx).value ++ dynamic.value))
}

