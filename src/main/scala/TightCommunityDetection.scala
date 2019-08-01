/**
  * Tight Community Detection in a social network
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import Utils._

// a class for formatting node features
case class NodeFeatureMap(nodeID: String, features: Array[String])

object TightCommunityDetection {
  def main(args: Array[String]): Unit = {

    // check for correct number of input arguments
    if (args.length != 5) {
      println("Usage: InputDir OutputDir numEdges validEdgeThreshold useMinimisedEdges")
    }
    // input directory
    val inputDir = args(0)

    // output directory
    val outputDir = args(1)

    // number of edges to consider
    val numEdges = args(2)

    // get the threshold of edgeweight to consider
    val validEdgeThreshold = args(3)

    // boolean value whther to use minimised edges
    val useMinimisedEdges = args(4)
    // configure spark and get spark context
    val sparkConfig = new SparkConf().setAppName("Tight Community Detector")
    val spark = new SparkSession.Builder()
                                .config(sparkConfig)
                                .getOrCreate()
    val sc = spark.sparkContext

    // get all the implicit function of spark
    import spark.implicits._

    // list of ego node IDs used in the project
    val egoIDs = Array(0, 107, 1684, 1912, 3437, 348, 3980, 414, 686, 698)

    // initializing global variables to use the values outside the for loop block
    var nodeFeaturesMapping = Seq.empty[NodeFeatureMap]
    var fmap: RDD[String] = sc.emptyRDD[String]
    var totalFeatureDf: RDD[(Int, Array[String])] = sc.emptyRDD[(Int, Array[String])]
    var finalIndividualMap: RDD[(Int, Array[String])] = sc.emptyRDD[(Int, Array[String])]


    for (egoID <- egoIDs) {
      // get the directory name of all features to loop through
      val featnameFileName = "/%d.featnames*".format(egoID)
      val featFileName = "/%d.feat".format(egoID)
      val egoFeatFileName = "/%d.egofeat".format(egoID)

      // read the featname file and get the features
      val features = sc.textFile(inputDir + featnameFileName).map(parseFeatures(_))

      // collect all the features at one place for each egoID
      nodeFeaturesMapping = nodeFeaturesMapping.union(Seq(NodeFeatureMap(egoID.toString, features.collect.toArray)))

      // get only the unique features
      fmap = fmap.union(features).distinct

      // get the feature matrix from feat file
      val featureMatrixDF = sc.textFile(inputDir + featFileName).map(getFeatureMatrix(_, -1))
      totalFeatureDf = totalFeatureDf.union(featureMatrixDF)

      // get unique features for each ego Node
      val uniqueFeatureForCurrentEgo = features.collect

      // apply  featureMatrix mask on unique features to map the unique feature name
      val individual_map = featureMatrixDF.map {
        case (a, featMap) => (a, uniqueFeatureForCurrentEgo.zip(featMap).collect { case (v, "1") => v }.toArray)
      }

      // aggregate all the features names
      finalIndividualMap = finalIndividualMap.union(individual_map)
    }
    // group all the neighbors of a vertex together
    val finalFeatureMap = finalIndividualMap.groupByKey.mapValues(_.flatten)

    // initialize a graph with all the edges
    val graph = GraphLoader.edgeListFile(sc, inputDir + "/*.edges")

    // get all the neighbors for each node
    val neighborsForEachNode = graph.collectNeighbors(EdgeDirection.Either).join(graph.vertices)
                                        .map {
                                          case (vid, t) =>
                                            val neighbors = t._1.map(_._1)
                                            (vid, neighbors)
                                        }

    // splitting all the neighbors so that we can map them on to get the edge weights
    val neighborDF = neighborsForEachNode.toDF("vertex_ID", "neighbors")
                                    .withColumn("neighbors", explode($"neighbors"))

    // get the nodeIDs (to, from) from neighbor DataFrame
    val neighborRDD = neighborDF.collect.map(t => (t.getLong(0), t.getLong(1)))

    // trying to minimize the dataset to get the results early by taking less edges
    var minimisedEdges = neighborRDD

    if(useMinimisedEdges.toBoolean){
      minimisedEdges = neighborRDD.take(numEdges.toInt)
    }

    // get the edge weights of all the edges and filter based on threshold
    val edgeWeights100 = minimisedEdges.map { t =>
      val weight = getIntersection(getFeat(t._1, finalFeatureMap), getFeat(t._2, finalFeatureMap))
      (t._1, t._2, weight)
    }.filter(_._3 > validEdgeThreshold.toInt)

    // converting edge weights to RDD and then to DataFrame for easier processing
    val edgeRdd = sc.parallelize(edgeWeights100)
    val edgeDF = edgeRdd.toDF("To","From","Weight")

    // getting the source and destination nodes and then finding unique list of vertices
    val tos = edgeRdd.map(t => t._1)
    val froms = edgeRdd.map(t => t._2)
    val vertexRdd = tos.union(froms).distinct

    // preparation of vertices and edges to make a working vertex RDD required for graphX
    val vertexArray = vertexRdd.map( t => ( 1L,  t ) )
    val edgeArray = edgeRdd.map( t => Edge(t._1, t._2, t._3))

    // making the new graph with filtered edges
    val graphNew = Graph(vertexArray, edgeArray)

    // Instantiate the algorithm instance
    val cliqueFinder = new BronKerbosch(sc, graphNew)

    // getting the cliques returned
    val detectedCliques = cliqueFinder.runAlgorithm

    // making the detected Cliques as RDD to write easily on file
    val detectedCliquesDF = sc.parallelize(detectedCliques.map(_.toArray).toSeq).toDF

    detectedCliquesDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save(outputDir)
  }

}
