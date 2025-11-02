// 代码生成时间: 2025-11-03 06:47:33
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class SentimentAnalysis {

    /**
     * Main entry point for the Sentiment Analysis application.
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        // Set up Spark configuration and context
        SparkConf conf = new SparkConf().setAppName("SentimentAnalysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // Load the input data
            String inputFile = "path_to_input_data"; // Replace with the path to your input data
            JavaRDD<String> data = sc.textFile(inputFile).cache();

            // Define the number of partitions
            int numPartitions = 100;
            data.repartition(numPartitions);

            // Split the documents into words
            JavaRDD<String> words = data.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

            // Remove all non-word characters and convert to lower case
            JavaRDD<String> filteredWords = words.filter(s -> s.matches("[a-zA-Z]+"));
            JavaRDD<String> lowerCaseWords = filteredWords.map(s -> s.toLowerCase());

            // Split the documents into sentences
            JavaRDD<List<String>> sentences = data.map(s -> Arrays.asList(s.split(" ")));

            // Model training
            Word2VecModel word2VecModel = Word2Vec.train(sentences.rdd(), numPartitions, 1, 0.025, 20);

            // Perform sentiment analysis
            JavaRDD<Tuple2<Object, Double>> sentimentScores = data.mapPartitions(iter -> {
                SentimentAnalyzer analyzer = new SentimentAnalyzer(word2VecModel);
                return analyzer.analyzeSentiment(iter);
            });

            // Collect and print the sentiment scores
            sentimentScores.foreach(tuple -> System.out.println(tuple));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }
    }

    /**
     * A helper class to perform sentiment analysis using the Word2Vec model.
     */
    static class SentimentAnalyzer {
        private Word2VecModel model;

        public SentimentAnalyzer(Word2VecModel model) {
            this.model = model;
        }

        /**
         * Analyze sentiment for each sentence.
         * @param iter An iterator over the sentences.
         * @return A tuple containing the sentence and its sentiment score.
         */
        public Iterable<Tuple2<Object, Double>> analyzeSentiment(Iterator<String> iter) {
            List<Tuple2<Object, Double>> result = new ArrayList<>();
            while (iter.hasNext()) {
                String sentence = iter.next();
                List<String> words = Arrays.asList(sentence.split(" "));
                List<Vector> vectors = new ArrayList<>();
                for (String word : words) {
                    if (model.getVectors().containsKey(word)) {
                        vectors.add(model.getVectors().get(word));
                    }
                }

                if (!vectors.isEmpty()) {
                    double sentimentScore = vectors.stream()
                            .mapToDouble(Vector::dot)
                            .sum() / vectors.size();
                    result.add(new Tuple2<>(sentence, sentimentScore));
                }
            }
            return result;
        }
    }
}
