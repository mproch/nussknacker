package pl.touk.nussknacker.engine.types

import java.util.regex.Pattern

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._
import pl.touk.nussknacker.engine.api.{Documentation, Hidden, HideToString, ParamName}
import pl.touk.nussknacker.engine.api.process.{ClassExtractionSettings, ClassMemberPatternPredicate}
import pl.touk.nussknacker.engine.api.typed.ClazzRef
import pl.touk.nussknacker.engine.api.typed.typing.Typed
import pl.touk.nussknacker.engine.definition.TypeInfos.{ClazzDefinition, MethodInfo, Parameter}

import scala.annotation.meta.getter
import scala.concurrent.Future
import scala.reflect.runtime.universe._

class EspTypeUtilsSpec extends FunSuite with Matchers {

  val signatures = Table(("signature", "value", "matches"),
    (java.lang.Boolean.TYPE, classOf[java.lang.Boolean], true),
    (java.lang.Long.TYPE, classOf[java.lang.Long], true),
    (java.lang.Integer.TYPE, classOf[java.lang.Integer], true),
    (classOf[java.lang.Long], classOf[java.lang.Integer], true),
    (classOf[java.lang.Long], java.lang.Integer.TYPE, true),
    (java.lang.Long.TYPE, java.lang.Integer.TYPE, true),
    (java.lang.Long.TYPE, classOf[java.lang.Integer], true),
    (java.lang.Long.TYPE, classOf[java.lang.Integer], true),

    (java.lang.Character.TYPE, classOf[java.lang.Character], true),
    (classOf[java.lang.Number], classOf[java.lang.Integer], true),
    (java.lang.Integer.TYPE, classOf[java.lang.Long], false)
  )

  test("should check if signature is possible") {

    forAll(signatures) { (signature, value, matches) =>
      EspTypeUtils.signatureElementMatches(signature, value) shouldBe matches
    }
  }

  case class SampleClass(foo: Int, bar: String) extends SampleAbstractClass with SampleInterface

  class Returning {
    def futureOfList: Future[java.util.List[SampleClass]] = ???
  }

  test("should extract generic return type parameters") {

    val method = classOf[Returning].getMethod("futureOfList")

    val extractedType = EspTypeUtils.getGenericType(method.getGenericReturnType).get

    extractedType.clazz shouldBe classOf[java.util.List[_]]
    extractedType.params shouldBe List(ClazzRef[SampleClass])
  }

  test("should extract public fields from scala case class") {
    val infos = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(Typed[SampleClass]))(ClassExtractionSettings.Default)
    val sampleClassInfo = infos.find(_.clazzName.refClazzName.contains("SampleClass")).get

    sampleClassInfo.methods shouldBe Set(
      MethodInfo("foo", List.empty, ClazzRef(Integer.TYPE), None),
      MethodInfo("bar", List.empty, ClazzRef[String], None),
      MethodInfo("toString", List(), ClazzRef[String], None)
    )
  }

  test("shoud detect java beans and fields in java class") {
    EspTypeUtils.clazzDefinition(classOf[JavaSampleClass])(ClassExtractionSettings.Default).methods shouldBe Set(
      MethodInfo("getNotProperty", List(Parameter("foo", ClazzRef[Int])), ClazzRef[String], None),
      MethodInfo("bar", List(), ClazzRef[String], None),
      MethodInfo("getBeanProperty", List(), ClazzRef[String], None),
      MethodInfo("beanProperty", List(), ClazzRef[String], None),
      MethodInfo("isBooleanProperty", List(), ClazzRef[Boolean], None),
      MethodInfo("booleanProperty", List(), ClazzRef[Boolean], None),
      MethodInfo("foo", List(), ClazzRef(Integer.TYPE), None),
      MethodInfo("toString", List(), ClazzRef[String], None)
    )

  }

  test("should skip blacklisted properties") {
    val testCasses = Table(("class", "className"),
      (Typed[SampleClass], "SampleClass"),
      (Typed[JavaSampleClass], "JavaSampleClass")
    )

    val testClassPatterns = Table("classPattern",
      ".*SampleClass",
      ".*SampleAbstractClass",
      ".*SampleInterface"
    )

    forAll(testCasses) { (clazz, clazzName) =>
      forAll(testClassPatterns) { classPattern =>
        val infos = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(clazz))(ClassExtractionSettings(Seq(
          ClassMemberPatternPredicate(Pattern.compile(classPattern), Pattern.compile("ba.*")),
          ClassMemberPatternPredicate(Pattern.compile(classPattern), Pattern.compile("get.*")),
          ClassMemberPatternPredicate(Pattern.compile(classPattern), Pattern.compile("is.*"))
        )))
        val sampleClassInfo = infos.find(_.clazzName.refClazzName.contains(clazzName)).get

        sampleClassInfo.methods shouldBe Set(
          MethodInfo("toString", List(), ClazzRef[String], None),
          MethodInfo("foo", List.empty, ClazzRef(Integer.TYPE), None)
        )
      }
    }
  }

  test("should extract parameters from embedded lists") {

    val typeUtils = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(Typed[Embeddable]))(ClassExtractionSettings.Default)

    typeUtils.find(_.clazzName == ClazzRef[TestEmbedded]) shouldBe Some(ClazzDefinition(ClazzRef[TestEmbedded], Set(
      MethodInfo("string", List(), ClazzRef[String], None),
      MethodInfo("javaList", List(), ClazzRef.fromDetailedType[java.util.List[String]], None),
      MethodInfo("scalaList", List(), ClazzRef.fromDetailedType[List[String]], None),
      MethodInfo("toString", List(), ClazzRef[String], None)
    )))

  }

  test("should not discover hidden fields") {
    val typeUtils = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(Typed[ClassWithHiddenFields]))(ClassExtractionSettings.Default)

    typeUtils.find(_.clazzName == ClazzRef[ClassWithHiddenFields]) shouldBe Some(ClazzDefinition(ClazzRef[ClassWithHiddenFields], Set(
      MethodInfo("normalField", List(), ClazzRef[String], None),
      MethodInfo("normalParam", List(), ClazzRef[String], None),
      MethodInfo("toString", List(), ClazzRef[String], None)
    )))
  }

  test("should skip toString method when HideToString implemented") {
    val hiddenToStringClasses = Table("class", classOf[JavaBannedToStringClass], classOf[BannedToStringClass])
    forAll(hiddenToStringClasses) { EspTypeUtils.clazzDefinition(_)(ClassExtractionSettings.Default)
      .methods.map(_.name) shouldNot contain("toString")
    }
  }

  class BannedToStringClass extends HideToString

  case class ScalaSampleDocumentedClass() {

    val field1: Long = 123L

    //we use this @getter annotation here, because vals in case classes are translated by default to private field and public getter method for that field
    //we just annotate public getter method (that will be created by scalac) for field2
    //more here: https://www.scala-lang.org/api/current/scala/annotation/meta/index.html
    @(Documentation @getter)(description = ScalaSampleDocumentedClass.field2Docs)
    val field2: Long = 234L

    def foo(fooParam1: String): Long = {
      0L
    }

    def bar(@ParamName("barparam1") barparam1: Long): String = {
      ""
    }
    @Documentation(description = ScalaSampleDocumentedClass.bazDocs)
    def baz(@ParamName("bazparam1") bazparam1: String, @ParamName("bazparam2") bazparam2: Int): Long = {
      0L
    }

    @Documentation(description = ScalaSampleDocumentedClass.quxDocs)
    def qux(quxParam1: String): Long = {
      0L
    }
  }

  case class TestEmbedded(string: String, javaList: java.util.List[String], scalaList: List[String])

  class Embeddable {

    def data: Future[List[TestEmbedded]] = ???

  }

  case class ClassWithHiddenFields(@(Hidden @getter)imnothereCaseClassParam: String, normalParam: String) {

    @Hidden
    def imnothereMethod(par: String): String = par

    @(Hidden @getter)
    val imnothereField: String = ""

    val normalField: String = ""
  }

  object ScalaSampleDocumentedClass {
    final val field2Docs = "This is sample documentation for field2 field"
    final val bazDocs = "This is sample documentation for baz method"
    final val quxDocs = "This is sample documentation for qux method"
  }

  test("should extract description and params from method") {
    val scalaExtractedInfo = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(Typed[ScalaSampleDocumentedClass]))(ClassExtractionSettings.Default)
    val scalaClazzInfo = scalaExtractedInfo.find(_.clazzName == ClazzRef[ScalaSampleDocumentedClass]).get

    val javaExtractedInfo = TypesInformationExtractor.clazzAndItsChildrenDefinition(List(Typed[JavaSampleDocumentedClass]))(ClassExtractionSettings.Default)
    val javaClazzInfo = javaExtractedInfo.find(_.clazzName == ClazzRef[JavaSampleDocumentedClass]).get

    val table = Table(
      ("methodInfo"),
      //FIXME: scala 2.11, 2.12 have different behaviour - named parameters are extracted differently :/
      //(MethodInfo("foo", parameters = List(param[String]("fooParam1")), refClazz = ClazzRef[Long], description = None)),
      (MethodInfo("bar", parameters = List(param[Long]("barparam1")), refClazz = ClazzRef[String], description = None)),
      (MethodInfo("baz", parameters = List(param[String]("bazparam1"), param[Int]("bazparam2")), refClazz = ClazzRef[Long], description = Some(ScalaSampleDocumentedClass.bazDocs))),
      //FIXME: scala 2.11, 2.12 have different behaviour - named parameters are extracted differently :/
      //(MethodInfo("qux", parameters = List(param[String]("quxParam1")), refClazz = ClazzRef[Long], description = Some(ScalaSampleDocumentedClass.quxDocs))),
      (MethodInfo("field1", parameters = List.empty, refClazz = ClazzRef[Long], description = None)),
      (MethodInfo("field2", parameters = List.empty, refClazz = ClazzRef[Long], description = Some(ScalaSampleDocumentedClass.field2Docs)))
    )
    forAll(table){ methodInfo =>
        scalaClazzInfo.methods.find(_.name == methodInfo.name) shouldBe Some(methodInfo)
        javaClazzInfo.methods.find(_.name == methodInfo.name) shouldBe Some(methodInfo)
    }
  }

  private def param[T: TypeTag](name: String): Parameter = {
    Parameter(name, ClazzRef.fromDetailedType[T])
  }

}
