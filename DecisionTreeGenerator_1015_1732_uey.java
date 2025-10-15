// 代码生成时间: 2025-10-15 17:32:36
import org.apache.spark.SparkConf;
# NOTE: 重要实现细节
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
# FIXME: 处理边界情况
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DecisionTreeGenerator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DecisionTreeGenerator");
# NOTE: 重要实现细节
        JavaSparkContext sc = new JavaSparkContext(conf);
# 扩展功能模块
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        try {
            // Load data from a file
# 扩展功能模块
            List<LabeledPoint> data = DecisionTreeGenerator.loadData(sc);
# 增强安全性

            // Split dataset into training (70%) and test (30%) sets
# FIXME: 处理边界情况
            List<LabeledPoint>[] splits = data.split((int) (0.7 * data.size()));
            List<LabeledPoint> trainingData = splits[0];
            List<LabeledPoint> testData = splits[1];

            // Train a DecisionTree model
            DecisionTreeModel model = DecisionTree.trainRegressor(trainingData);

            // Evaluate model on test instances and compute test error
            List<Prediction> labelAndPreds = DecisionTreeGenerator.transform(testData, model);
            long testErr = labelAndPreds.stream().filter(p -> p.label != p.prediction).count();
            System.out.println("Test Error = " + testErr + " / " + testData.size());
            System.out.println("Test Error = 