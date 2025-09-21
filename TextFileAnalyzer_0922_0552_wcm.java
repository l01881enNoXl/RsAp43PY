// 代码生成时间: 2025-09-22 05:52:55
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TextFileAnalyzer {

    // Entry point for the application
    public static void main(String[] args) {
        // Check if the arguments provided are sufficient
        if (args.length < 1) {
            System.err.println("Usage: TextFileAnalyzer <inputPath>");
            System.exit(-1);
        }

        // Initialize Spark context
        SparkConf conf = new SparkConf().setAppName("TextFileAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the text file into a JavaRDD
        String inputPath = args[0];
        JavaRDD<String> textFile = sc.textFile(inputPath);

        try {
            // Perform text analysis
            analyzeTextFile(textFile);
        } catch (Exception e) {
            // Handle any exceptions that occur during analysis
            e.printStackTrace();
        } finally {
            // Stop the Spark context
            sc.close();
        }
    }

    // Method to analyze the text file
    private static void analyzeTextFile(JavaRDD<String> textFile) {
        // Calculate the total number of lines
        long totalLines = textFile.count();
        System.out.println("Total lines: " + totalLines);

        // Count the number of non-empty lines
        long nonEmptyLines = textFile.filter(line -> !line.isEmpty()).count();
        System.out.println("Non-empty lines: " + nonEmptyLines);

        // Count the number of lines containing a specific word
        String wordToSearch = "example"; // This can be made configurable
        long linesWithWord = textFile.filter(line -> line.contains(wordToSearch)).count();
        System.out.println("Lines containing '