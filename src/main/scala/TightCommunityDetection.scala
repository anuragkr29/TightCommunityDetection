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
case class nodeFeatureMap(nodeID: String, features: Array[String])

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
    val spconf = new SparkConf().setAppName("Tight Community Detector")
    val spark = new SparkSession.Builder()
                                .config(spconf)
                                .getOrCreate()
    val sc = spark.sparkContext

    // get all the implicit function of spark
    import spark.implicits._

    // list of ego node IDs used in the project
    val egoIDs = Array(0, 107, 1684, 1912, 3437, 348, 3980, 414, 686, 698)

    // initializing clobal variables to use the values outside the for loop block
    var nodeFeaturesMapping = Seq.empty[nodeFeatureMap]
    var fmap: RDD[String] = sc.emptyRDD[String]
    var totalFeatureDf: RDD[(Int, Array[String])] = sc.emptyRDD[(Int, Array[String])]
    var final_individual_map: RDD[(Int, Array[String])] = sc.emptyRDD[(Int, Array[String])]


    for (egoID <- egoIDs) {
      // get the directory name of all features to loop through
      var FeatnameFileName = "/%d.featnames*".format(egoID)
      var FeatFileName = "/%d.feat".format(egoID)
      var egoFeatFileName = "/%d.egofeat".format(egoID)

      // read the featname file and get the features
      var features = sc.textFile(inputDir + FeatnameFileName).map(parseFeatures(_))

      // collect all the features at one place for each egoID
      nodeFeaturesMapping = nodeFeaturesMapping.union(Seq(nodeFeatureMap(egoID.toString, features.collect.toArray)))

      // get only the unique features
      fmap = fmap.union(features).distinct

      // get the feature matrix from feat file
      var featureMatrixdf = sc.textFile(inputDir + FeatFileName).map(getFeatureMatrix(_, -1))
      totalFeatureDf = totalFeatureDf.union(featureMatrixdf)

      // get unique features for each ego Node
      var uniqueFeatureForCurrentEgo = features.collect

      // apply  featureMatrix mask on uniquefeatures to map the unique feature name
      var individual_map = featureMatrixdf.map {
        case (a, featmap) => (a, uniqueFeatureForCurrentEgo.zip(featmap).collect { case (v, "1") => v }.toArray)
      }

      // aggregate all the features names
      final_individual_map = final_individual_map.union(individual_map)
      print(inputDir + FeatFileName)
      System.exit(0)
    }
    // group all the neighbors of a vertex together
    var final_feature_map = final_individual_map.groupByKey.mapValues(_.flatten)

    // initialize a graph with all the edges
    val graph = GraphLoader.edgeListFile(sc, inputDir + "/*.edges")

    // get all the neighbors for each node
    val neighbors_for_each_node = graph.collectNeighbors(EdgeDirection.Either).join(graph.vertices)
                                        .map {
                                          case (vid, t) => {
                                                              val neighbors = t._1.map(_._1)
                                                              (vid, neighbors)
                                          }
                                        }

    // splitting all the neighbors so that we can map them on to get the edge weights
    val neighbor_df = neighbors_for_each_node.toDF("vertex_ID", "neighbors")
                                    .withColumn("neighbors", explode($"neighbors"))

    // get the nodeIDs (to, from) from neighbor dataframe
    val neighbor_rdd = neighbor_df.collect.map(t => (t.getLong(0), t.getLong(1)))

    // trying to minimize the dataset to get the results early by taking less edges
    var minimisedEdges = neighbor_rdd

    if(useMinimisedEdges.toBoolean){
      minimisedEdges = neighbor_rdd.take(numEdges.toInt)
    }

    // get the edge weights of all the edges and filter based on threshold
    val edgeWeights100 = minimisedEdges.map { case t => {
                        val weight = getIntersection(getFeat(t._1, final_feature_map), getFeat(t._2, final_feature_map))
                        (t._1, t._2, weight)
        }
    }.filter(_._3 > validEdgeThreshold.toInt)

    // converting edgeweights to RDD and then to Dataframe for easier processing
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
    var detectedCliques = cliqueFinder.runAlgorithm

    // making the detected Cliques as RDD to write easily on file
    var detectedCliquesDF = sc.parallelize(detectedCliques.map(_.toArray).toSeq).toDF

    detectedCliquesDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save(outputDir)
  }

}
