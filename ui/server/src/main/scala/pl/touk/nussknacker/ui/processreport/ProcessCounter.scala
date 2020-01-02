package pl.touk.nussknacker.ui.processreport

import io.circe.generic.JsonCodec
import pl.touk.nussknacker.engine.api.ProcessAdditionalFields
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.canonicalgraph.canonicalnode._
import pl.touk.nussknacker.engine.graph.node.{NodeData, SubprocessInputDefinition}
import pl.touk.nussknacker.processCounts.{CountsForProcess, RawCount}
import pl.touk.nussknacker.ui.process.subprocess.SubprocessRepository
import shapeless.syntax.typeable._

class ProcessCounter(subprocessRepository: SubprocessRepository) {


  def computeCounts(canonicalProcess: CanonicalProcess, counts: CountsForProcess) : Map[String, NodeCount] = {

    def computeCounts(prefixes: List[String])(nodes: Iterable[CanonicalNode]) : Map[String, NodeCount] = {

      val computeCountsSamePrefixes = computeCounts(prefixes) _

      def nodeCount(id: String, nodeData: NodeData, subprocessCounts: Map[String, NodeCount] = Map()) : NodeCount =
        nodeCountOption(Some(id), nodeData, subprocessCounts)

      def nodeCountOption(id: Option[String], nodeData: NodeData, subprocessCounts: Map[String, NodeCount] = Map()) : NodeCount = {
        val countId = (prefixes ++ id).mkString("-")
        val count = counts.countsForNode(countId, nodeData).getOrElse(RawCount(0L, 0L, None))
        NodeCount(count.all, count.errors, count.additionalInfo, subprocessCounts)
      }

      nodes.flatMap {
        //TODO: this is a bit of a hack. Metric for subprocess input is counted in node with subprocess occurrence id...
        case FlatNode(nd@SubprocessInputDefinition(id, _, _)) => Map(id -> nodeCountOption(None, nd))
        case FlatNode(node) => Map(node.id -> nodeCount(node.id, node))
        case FilterNode(node, nextFalse) => computeCountsSamePrefixes(nextFalse) + (node.id -> nodeCount(node.id, node))
        case SwitchNode(node, nexts, defaultNext) =>
          computeCountsSamePrefixes(nexts.flatMap(_.nodes)) ++ computeCountsSamePrefixes(defaultNext) + (node.id -> nodeCount(node.id, node))
        case SplitNode(node, nexts) => computeCountsSamePrefixes(nexts.flatten) + (node.id -> nodeCount(node.id, node))
        case Subprocess(node, outputs) =>
          //TODO: validate that process exists
          val subprocess = getSubprocess(canonicalProcess.metaData.subprocessVersions, node.ref.id).get
          computeCountsSamePrefixes(outputs.values.flatten) + (node.id -> nodeCount(node.id, node,
            computeCounts(prefixes :+ node.id)(subprocess.nodes)))
      }.toMap

    }
    val valuesWithoutGroups = computeCounts(List())(canonicalProcess.nodes)

    val valuesForGroups: Map[String, NodeCount] = computeValuesForGroups(valuesWithoutGroups, canonicalProcess)
    valuesWithoutGroups ++ valuesForGroups
  }

  private def getSubprocess(subprocessVersions: Map[String, Long], subprocessId: String): Option[CanonicalProcess] = {
    val subprocess = subprocessVersions.get(subprocessId) match {
      case Some(version) => subprocessRepository.get(subprocessId, version)
      case None => subprocessRepository.get(subprocessId)
    }
    subprocess.map(_.canonical)
  }

  private def computeValuesForGroups(valuesWithoutGroups: Map[String, NodeCount], canonicalProcess: CanonicalProcess) = {
    val groups = canonicalProcess.metaData.additionalFields.flatMap(_.cast[ProcessAdditionalFields]).map(_.groups).getOrElse(Set())

    groups.map { group =>
      val valuesForGroup = valuesWithoutGroups.filterKeys(nodeId => group.nodes.contains(nodeId))
      val all = valuesForGroup.values.map(_.all).max
      val errors = valuesForGroup.values.map(_.errors).max
      //TODO:??
      group.id -> NodeCount(all, errors, None)
    }.toMap
  }
}

@JsonCodec case class NodeCount(all: Long,
                                errors: Long,
                                additionalInfo: Option[String] = None,
                                subprocessCounts: Map[String, NodeCount] = Map())