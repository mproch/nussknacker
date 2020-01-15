package pl.touk.nussknacker.engine.types

import java.lang.reflect._

import cats.data.StateT
import cats.effect.IO
import org.apache.commons.lang3.{ClassUtils, StringUtils}
import pl.touk.nussknacker.engine.api.process.ClassExtractionSettings
import pl.touk.nussknacker.engine.api.typed.ClazzRef
import pl.touk.nussknacker.engine.api.{Documentation, Hidden, HideToString, ParamName}
import pl.touk.nussknacker.engine.definition.TypeInfos.{ClazzDefinition, MethodInfo, Parameter}
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

import scala.concurrent.Future

object EspTypeUtils {

  private val blacklistedMethods: Set[String] = {
    (methodNames(classOf[ScalaCaseClassStub.DumpCaseClass]) ++
      methodNames(ScalaCaseClassStub.DumpCaseClass.getClass)).toSet.filterNot(
      //sometimes toString can be pretty useful...
      List("toString").contains
    )
  }

  def clazzDefinition(clazz: Class[_])
                     (implicit settings: ClassExtractionSettings): ClazzDefinition =
    ClazzDefinition(ClazzRef(clazz), getPublicMethodAndFields(clazz))

  def extractParameterType(p: java.lang.reflect.Parameter, classesToExtractGenericFrom: Class[_]*): Class[_] =
    if (classesToExtractGenericFrom.contains(p.getType)) {
      val parameterizedType = p.getParameterizedType.asInstanceOf[ParameterizedType]
      parameterizedType.getActualTypeArguments.apply(0) match {
        case a: Class[_] => a
        case b: ParameterizedType => b.getRawType.asInstanceOf[Class[_]]
      }
    } else {
      p.getType
    }

  def getCompanionObject[T](klazz: Class[T]): T = {
    klazz.getField("MODULE$").get(null).asInstanceOf[T]
  }

  def getGenericType(genericReturnType: Type): Option[ClazzRef] = {
    val hasGenericReturnType = genericReturnType.isInstanceOf[ParameterizedTypeImpl]
    if (hasGenericReturnType) inferGenericMonadType(genericReturnType.asInstanceOf[ParameterizedTypeImpl])
    else None
  }

  //TODO: what is *really* needed here?? is it performant enough??
  def signatureElementMatches(signatureType: Class[_], passedValueClass: Class[_]): Boolean = {
    def unbox(typ: Class[_]) = if (ClassUtils.isPrimitiveWrapper(typ)) ClassUtils.wrapperToPrimitive(typ) else typ

    ClassUtils.isAssignable(passedValueClass, signatureType, true) ||
      ClassUtils.isAssignable(unbox(passedValueClass), unbox(signatureType), true)
  }

  private def methodNames(clazz: Class[_]): List[String] = {
    clazz.getMethods.map(_.getName).toList
  }

  private def getPublicMethodAndFields(clazz: Class[_])
                                      (implicit settings: ClassExtractionSettings): Set[MethodInfo] = {
    val methods = publicMethods(clazz)
    val fields = publicFields(clazz)
    methods ++ fields
  }

  private def publicMethods(clazz: Class[_])
                           (implicit settings: ClassExtractionSettings): Set[MethodInfo] = {
    val shouldHideToString = classOf[HideToString].isAssignableFrom(clazz)

    /* From getMethods javadoc: If this {@code Class} object represents an interface then the returned array
       does not contain any implicitly declared methods from {@code Object}.
       The same for primitives - we assume that languages like SpEL will be able to do boxing
       It could be significant only for toString, as we filter out other Object methods, but to be consistent...
     */
    val publicMethods = clazz.getMethods.toList ++ (if (clazz.isInterface || clazz.isPrimitive) classOf[Object].getMethods.toList else List.empty)

    val filteredMethods = publicMethods
      .filterNot(m => Modifier.isStatic(m.getModifiers))
      .filter(_.getAnnotation(classOf[Hidden]) == null)
      .filterNot(m => shouldHideToString && m.getName == "toString" && m.getParameterCount == 0)
      .filterNot(settings.isBlacklisted)
      .filter(m =>
        !blacklistedMethods.contains(m.getName) && !m.getName.contains("$")
      )

    filteredMethods.flatMap { method =>
      methodAccessMethods(method).map(toMethodInfo(_, method))
    }.toSet
  }

  private def toMethodInfo(name: String, method: Method)
    = MethodInfo(name, getParameters(method), getReturnClassForMethod(method), getNussknackerDocs(method))

  private def methodAccessMethods(method: Method) = {
    val isGetter = (method.getName.startsWith("get") || method.getName.startsWith("is")) && method.getParameterCount == 0
    if (isGetter)
      List(method.getName, StringUtils.uncapitalize(method.getName.replaceAll("^get|^is", ""))) else List(method.getName)
  }

  private def publicFields(clazz: Class[_])
                          (implicit settings: ClassExtractionSettings): Set[MethodInfo] = {
    val interestingFields = clazz.getFields
      .filterNot(f => Modifier.isStatic(f.getModifiers))
      .filter(_.getAnnotation(classOf[Hidden]) == null)
      .filterNot(settings.isBlacklisted)
      .filter(m =>
        !m.getName.contains("$")
      )
    interestingFields.map { field =>
      MethodInfo(field.getName, List.empty, getReturnClassForField(field), getNussknackerDocs(field))
    }.toSet
  }

  private def getReturnClassForMethod(method: Method): ClazzRef = {
    getGenericType(method.getGenericReturnType).orElse(extractClass(method.getGenericReturnType)).getOrElse(ClazzRef(method.getReturnType))
  }

  private def getParameters(method: Method): List[Parameter] = {
    for {
      param <- method.getParameters.toList
      annotationOption = Option(param.getAnnotation(classOf[ParamName]))
      name = annotationOption.map(_.value).getOrElse(param.getName)
      clazzRef = ClazzRef(param.getType)
    } yield Parameter(name, clazzRef)
  }

  private def getNussknackerDocs(accessibleObject: AccessibleObject): Option[String] = {
    Option(accessibleObject.getAnnotation(classOf[Documentation])).map(_.description())
  }

  private def getReturnClassForField(field: Field): ClazzRef = {
    getGenericType(field.getGenericType).orElse(extractClass(field.getType)).getOrElse(ClazzRef(field.getType))
  }

  //TODO this is not correct for primitives and complicated hierarchies, but should work in most cases
  //http://docs.oracle.com/javase/8/docs/api/java/lang/reflect/ParameterizedType.html#getActualTypeArguments--
  private def inferGenericMonadType(genericMethodType: ParameterizedTypeImpl): Option[ClazzRef] = {
    val rawType = genericMethodType.getRawType

    if (classOf[StateT[IO, _, _]].isAssignableFrom(rawType)) {
      val returnType = genericMethodType.getActualTypeArguments.apply(3) // it's IndexedStateT[IO, ContextWithLazyValuesProvider, ContextWithLazyValuesProvider, A]
      extractClass(returnType)
    }
    else if (classOf[Future[_]].isAssignableFrom(rawType)) {
      val futureGenericType = genericMethodType.getActualTypeArguments.apply(0)
      extractClass(futureGenericType)
    }
    else if (classOf[Option[_]].isAssignableFrom(rawType)) {
      val optionGenericType = genericMethodType.getActualTypeArguments.apply(0)
      extractClass(optionGenericType)
    }
    else None
  }

  private def extractClass(futureGenericType: Type): Option[ClazzRef] = {
    futureGenericType match {
      case t: Class[_] => Some(ClazzRef(t))
      case t: ParameterizedTypeImpl => Some(extractGenericParams(t))
      case t => None
    }
  }

  private def extractGenericParams(paramsType: ParameterizedTypeImpl): ClazzRef = {
    val rawType = paramsType.getRawType
    if (classOf[java.util.Collection[_]].isAssignableFrom(rawType)) {
      ClazzRef(rawType, paramsType.getActualTypeArguments.toList.flatMap(extractClass))
    } else if (classOf[scala.collection.Iterable[_]].isAssignableFrom(rawType)) {
      ClazzRef(rawType, paramsType.getActualTypeArguments.toList.flatMap(extractClass))
    } else ClazzRef(rawType)
  }

  private object ScalaCaseClassStub {

    case class DumpCaseClass()

    object DumpCaseClass

  }

}