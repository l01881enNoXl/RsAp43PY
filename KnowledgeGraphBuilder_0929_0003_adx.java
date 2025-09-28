// 代码生成时间: 2025-09-29 00:03:25
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * This class provides functionality to build a knowledge graph using Apache Spark.
 * It uses Spark's DataFrame and RDD APIs to process data and create graph representations.
 */
public class KnowledgeGraphBuilder {

    private SparkSession spark;

    /**
     * Constructor to initialize the Spark session.
     * @param spark Spark session.
     */
    public KnowledgeGraphBuilder(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Method to load data into Spark DataFrame.
     * @param path Path to the data file.
     * @return DataFrame containing the data.
     */
    public Dataset<Row> loadData(String path) {
        return spark.read().json(path);
    }

    /**
     * Method to process data and create edges for the knowledge graph.
     * @param data DataFrame containing the data.
     * @return RDD of Tuple2 representing the edges of the graph.
     */
    public JavaRDD<Tuple2<String, String>> createGraphEdges(Dataset<Row> data) {
        return data.javaRDD()
                .flatMap(row -> {
                    List<String> edges = Arrays.asList(
                            row.getAs("source") + "-" + row.getAs("target"),
                            row.getAs("target") + "-" + row.getAs("source")
                    );
                    return edges.iterator();
                });
    }

    /**
     * Method to group and count the occurrences of each edge.
     * @param edges RDD of Tuple2 representing the edges.
     * @return RDD of Tuple2 containing the edge and its count.
     */
    public JavaRDD<Tuple2<String, Integer>> countEdges(JavaRDD<Tuple2<String, String>> edges) {
        return edges
                .mapToPair(edge -> new Tuple2<>(edge._1(), 1))
                .reduceByKey(Integer::sum);
    }

    /**
     * Main method to run the knowledge graph building process.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("KnowledgeGraphBuilder")
                .getOrCreate();

        KnowledgeGraphBuilder builder = new KnowledgeGraphBuilder(spark);

        if (args.length < 1) {
            System.err.println("Usage: KnowledgeGraphBuilder <data-path>");
            System.exit(1);
        }

        try {
            Dataset<Row> data = builder.loadData(args[0]);
            JavaRDD<Tuple2<String, String>> edges = builder.createGraphEdges(data);
            JavaRDD<Tuple2<String, Integer>> edgeCounts = builder.countEdges(edges);

            // Print the edge counts to the console
            edgeCounts.foreach(edge -> System.out.println(edge._1() + ": " + edge._2()));

        } catch (Exception e) {
            System.err.println("Error occurred while building the knowledge graph: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
