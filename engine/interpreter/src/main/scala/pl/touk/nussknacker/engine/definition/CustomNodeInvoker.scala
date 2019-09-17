package pl.touk.nussknacker.engine.definition

import pl.touk.nussknacker.engine.api.{LazyParameter, _}
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError.NodeId
import pl.touk.nussknacker.engine.api.typed.typing.{TypedClass, TypingResult, Unknown}
import pl.touk.nussknacker.engine.compile.ExpressionCompiler
import pl.touk.nussknacker.engine.expression.ExpressionEvaluator
import pl.touk.nussknacker.engine.graph.evaluatedparam
import pl.touk.nussknacker.engine.util.SynchronousExecutionContext

import scala.reflect.runtime.universe._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.runtime.universe

private[definition] case class ExpressionLazyParameter[T](nodeId: NodeId,
                                                          parameter: evaluatedparam.Parameter,
                                                          returnType: TypingResult) extends LazyParameter[T] {
}

private[definition] case class ProductLazyParameter[T, Y](arg1: LazyParameter[T], arg2: LazyParameter[Y]) extends LazyParameter[(T, Y)] {

  override def returnType: TypingResult = TypedClass(classOf[(T, Y)], List(arg1.returnType, arg2.returnType))

  def eval(lpi: LazyParameterInterpreter)(implicit ec: ExecutionContext): Context => Future[(T, Y)] = {
    val arg1Interpreter = lpi.createInterpreter(arg1)
    val arg2Interpreter = lpi.createInterpreter(arg2)
    ctx: Context =>
      val arg1Value = arg1Interpreter(ec, ctx)
      val arg2Value = arg2Interpreter(ec, ctx)
      arg1Value.flatMap(left => arg2Value.map((left, _)))

  }
}

private[definition] trait MappedLazyParameter[Y] extends LazyParameter[Y] {

  type InType

  def arg: LazyParameter[InType]

  def fun: InType => Y

  def eval(lpi: LazyParameterInterpreter)(implicit ec: ExecutionContext): Context => Future[Y] = {
    val argInterpreter = lpi.createInterpreter(arg)
    ctx: Context =>
        argInterpreter(ec, ctx).map(fun)

  }
}


private[definition] case class FixedLazyParameter[T:TypeTag](value: T) extends LazyParameter[T] {
  override def returnType: TypingResult = TypedClass[T]
}


trait CompilerLazyParameterInterpreter extends LazyParameterInterpreter {

  def deps: LazyInterpreterDependencies

  def metaData: MetaData

  override def createInterpreter[T](lazyInterpreter: LazyParameter[T]): (ExecutionContext, Context) => Future[T]
    = (ec: ExecutionContext, context: Context) => createInterpreter(ec, lazyInterpreter)(context)


  override def product[A, B](fa: LazyParameter[A], fb: LazyParameter[B]): LazyParameter[(A, B)] = {
    ProductLazyParameter(fa, fb)
  }

  override def map[T, Y: universe.TypeTag](parameter: LazyParameter[T], funArg: T => Y): LazyParameter[Y] = {
    new MappedLazyParameter[Y] {
      override type InType = T

      override def arg: LazyParameter[T] = parameter

      override def fun: T => Y = funArg

      override def returnType: TypingResult = TypedClass[Y]
    }
  }

  override def unit[T:TypeTag](value: T): LazyParameter[T] = FixedLazyParameter(value)

  //it's important that it's (...): (Context => Future[T])
  //and not e.g. (...)(Context) => Future[T] as we want to be sure when body is evaluated (in particular expression compilation)!
  private[definition] def createInterpreter[T](ec: ExecutionContext, definition: LazyParameter[T]): Context => Future[T] = {
    definition match {
      case ExpressionLazyParameter(nodeId, parameter, _) =>
        val compiledExpression = deps.expressionCompiler
          .compile(parameter.expression, Some(parameter.name), None, Unknown)(nodeId)
          .getOrElse(throw new IllegalArgumentException(s"Cannot compile ${parameter.name}")).expression
        val evaluator = deps.expressionEvaluator
        context: Context => evaluator.evaluate[T](compiledExpression, parameter.name, nodeId.id, context)(ec, metaData).map(_.value)(ec)
      case cli:ProductLazyParameter[_, _] =>
        cli.eval(this)(ec: ExecutionContext)
      case cli:MappedLazyParameter[T] =>
        cli.eval(this)(ec)
      case FixedLazyParameter(value) =>
        _ => Future.successful(value)
    }
  }

  override def syncInterpretationFunction[T](lazyInterpreter: LazyParameter[T]): Context => T = {

    implicit val ec: ExecutionContext = SynchronousExecutionContext.ctx
    val interpreter = createInterpreter(ec, lazyInterpreter)
    v1: Context => Await.result(interpreter(v1), deps.processTimeout)
  }


}

case class LazyInterpreterDependencies(expressionEvaluator: ExpressionEvaluator,
                                       expressionCompiler: ExpressionCompiler,
                                       processTimeout: FiniteDuration) extends Serializable

object CustomStreamTransformerExtractor extends AbstractMethodDefinitionExtractor[CustomStreamTransformer] {

  override protected val expectedReturnType: Option[Class[_]] = None

  override protected val additionalDependencies: Set[Class[_]] = Set[Class[_]](classOf[NodeId])

}



