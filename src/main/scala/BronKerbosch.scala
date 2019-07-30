/**
  * BronKerbosch algorithm for finding cliques
  */

import scala.collection.mutable.{ Set => MutableSet }
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps


class BronKerbosch[VD: ClassTag, ED: ClassTag](sc: SparkContext, inputGraph: Graph[VD, ED]) {


  private val sparkContext: SparkContext = sc;

  private val graph: Graph[VD, ED] = inputGraph;

  private val neighbourVerticesMap = graph.collectNeighborIds(EdgeDirection.Either)
    .collect().map(vertex => (vertex._1.asInstanceOf[Long], vertex._2.toSet))
    .toMap;

  def runAlgorithm = {

    val potentialClique = MutableSet[Long]()
    val candidates = graph.vertices
      .map(vertex => vertex._1.asInstanceOf[Long]).collect().toSet;
    val alreadyFound = MutableSet[Long]();
    val cliques = MutableSet[MutableSet[Long]]()
    findCliques(potentialClique, candidates, alreadyFound, cliques);
    cliques;
  }

  private def findCliques(potentialClique: MutableSet[Long],
                          candidates: Set[Long], alreadyFound: MutableSet[Long],
                          cliques: MutableSet[MutableSet[Long]]): Unit = {
    if (candidates.isEmpty && alreadyFound.isEmpty) {
      cliques.add(potentialClique)
    }
    val originalCandidates = candidates
    candidates.foreach { candidateVertex =>
    {
      val neighbourVertices = neighbourVerticesMap
        .getOrElse(candidateVertex, Set[Long]()).asInstanceOf[Set[Long]]

      findCliques(potentialClique ++ MutableSet(candidateVertex),
        candidates.intersect(neighbourVertices),
        alreadyFound.intersect(neighbourVertices),
        cliques)
    }
    }
  }

}

// Code used from https://github.com/shagunsodhani/MCE