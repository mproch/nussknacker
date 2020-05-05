package pl.touk.nussknacker.engine.spel.typer

import pl.touk.nussknacker.engine.api.process.ClassExtractionSettings
import pl.touk.nussknacker.engine.api.typed.typing._
import pl.touk.nussknacker.engine.definition.TypeInfos.{ClazzDefinition, MethodInfo}
import pl.touk.nussknacker.engine.types.EspTypeUtils

object TypeMethodReference {
  def apply(methodName: String, currentResults: List[TypingResult], params: List[TypingResult])(implicit settings: ClassExtractionSettings): Either[String, TypingResult] =
    new TypeMethodReference(methodName, currentResults, params).call
}

class TypeMethodReference(methodName: String, currentResults: List[TypingResult], calledParams: List[TypingResult]) {
  def call(implicit settings: ClassExtractionSettings): Either[String, TypingResult] =
    currentResults.headOption match {
      case Some(tc: SingleTypingResult) =>
        typeFromClazzDefinitions(extractClazzDefinitions(Set(tc)))
      case Some(TypedUnion(nestedTypes)) =>
        typeFromClazzDefinitions(extractClazzDefinitions(nestedTypes))
      case _ =>
        Right(Unknown)
    }

  private def extractClazzDefinitions(typedClasses: Set[SingleTypingResult])(implicit settings: ClassExtractionSettings): List[ClazzDefinition] =
    typedClasses.map(typedClass =>
      EspTypeUtils.clazzDefinition(typedClass.objType.klass)
    ).toList

  private def typeFromClazzDefinitions(clazzDefinitions: List[ClazzDefinition]): Either[String, TypingResult] = {
    val validatedType = for {
      nonEmptyClassDefinitions <- validateClassDefinitionsNonEmpty(clazzDefinitions).right
      nonEmptyMethods <- validateMethodsNonEmpty(nonEmptyClassDefinitions).right
      returnTypesForMatchingParams <- validateMethodParameterTypes(nonEmptyMethods).right
    } yield Typed(returnTypesForMatchingParams.toSet)

    validatedType match {
      case Left(None) => Right(Unknown) // we use Left(None) to indicate situation when we want to skip further validations because of lack of some knowledge
      case Left(Some(message)) => Left(message)
      case Right(returnType) => Right(returnType)
    }
  }

  private def validateClassDefinitionsNonEmpty(clazzDefinitions: List[ClazzDefinition]): Either[Option[String], List[ClazzDefinition]] =
    if (clazzDefinitions.isEmpty) Left(None) else Right(clazzDefinitions)

  private def validateMethodsNonEmpty(clazzDefinitions: List[ClazzDefinition]): Either[Option[String], List[MethodInfo]] = {
    def displayableType = clazzDefinitions.map(k => k.clazzName).map(_.display).mkString(", ")
    def isClass = clazzDefinitions.map(k => k.clazzName).exists(_.canBeSubclassOf(Typed[Class[_]]))
    clazzDefinitions.flatMap(_.methods.get(methodName).toList.flatten) match {
      //Static method can be invoked - we cannot find them ATM
      case Nil if isClass => Left(None)
      case Nil => Left(Some(s"Unknown method '$methodName' in $displayableType"))
      case methodInfoes => Right(methodInfoes)
    }
  }

  private def validateMethodParameterTypes(methodInfoes: List[MethodInfo]): Either[Option[String], List[TypingResult]] = {
    val returnTypesForMatchingMethods = methodInfoes.flatMap { m =>
      lazy val allMatching = m.parameters.map(_.refClazz).zip(calledParams).forall {
        case (declaredType, passedType) => passedType.canBeSubclassOf(declaredType)
      }
      if (m.parameters.size == calledParams.size && allMatching) Some(m.refClazz) else None
    }
    returnTypesForMatchingMethods match {
      case Nil =>
        def toSignature(params: List[TypingResult]) = params.map(_.display).mkString(s"$methodName(", ", ", ")")
        val methodVariances = methodInfoes.map(_.parameters.map(_.refClazz))
        Left(Some(s"Mismatch parameter types. Found: ${toSignature(calledParams)}. Required: ${methodVariances.map(toSignature).mkString(" or ")}"))
      case nonEmpty =>
        Right(nonEmpty)
    }
  }

}
