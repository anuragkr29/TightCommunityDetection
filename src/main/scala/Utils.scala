/**
  * Util function to help TightCommunityDetection
  */

import scala.util.matching.Regex
import org.apache.spark.rdd.RDD

object Utils {
  /**
    * A function to parse the features from the featname file
    * @param line - string to be parsed
    * @return - String - the feature name extracted
    */
  def parseFeatures(line: String): String = {
    val featurePattern: Regex = raw"\d+ (.*);anonymized feature (\d+)".r
    var matches = featurePattern.findAllIn(line)
    if (matches.isEmpty) {
      print("Parse error for %s".format(line))
      ""
    }
    else {
      return matches.group(1).replaceAll(";", "_") + "_" + matches.group(2)
    }
  }

  /**
    * Function to get the feature matrix
    * @param line - feature string containing binary values
    * @param selfid - node id corresponding to the feature values
    * @return - array of tuple containing nodeID and corresponding feature
    */
  def getFeatureMatrix(line: String, selfid: Int): (Int, Array[String]) = {
    var allents = line.split(" ")
    if (selfid != -1) {
      return (selfid, allents)
    }
    else {
      return (allents(0).toInt, allents.takeRight(allents.length - 1))
    }
  }

  /**
    * Function to get the neighbors of a node
    * @param node - node for which the neighbor is to be extracted
    * @param nodeFeatures - the RDD containing all the neighbors
    * @return - Array[String] - the list of neighbor nodeIDs
    */
  def getFeat(node: Long, nodeFeatures: RDD[(Int, Iterable[String])]): Array[String] = {
    val matched = nodeFeatures.filter(_._1 == node)
    if (matched.take(1).length > 0)
      matched.first._2.toArray
    else
      Array()
  }

  /**
    * Function to return number of common features in two string array
    * @param firstArray - first string array
    * @param secondArray - second string array
    * @return - Int - the number of common features
    */
  def getIntersection(firstArray: Array[String], secondArray: Array[String]): Int = {
    if (firstArray.length > 0 && secondArray.length > 0) {
      val intersect = firstArray.map(x => secondArray.count(y => y == x))
      (intersect.sum)
    } else
      (0)
  }
}
