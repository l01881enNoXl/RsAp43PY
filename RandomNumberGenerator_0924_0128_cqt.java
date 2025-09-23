// 代码生成时间: 2025-09-24 01:28:54
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Random;

public class RandomNumberGenerator {
    // 定义一个方法来生成随机数
    public static Tuple2<Integer, Integer> generateRandomNumber(int lowerBound, int upperBound) {
        Random random = new Random();
        int randomNumber = lowerBound + random.nextInt(upperBound - lowerBound + 1);
        return new Tuple2<>(lowerBound, randomNumber);
    }

    // 主方法，程序入口
    public static void main(String[] args) {
        // 初始化Spark配置和上下文
        SparkConf conf = new SparkConf().setAppName("RandomNumberGenerator").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 测试随机数生成器
            Tuple2<Integer, Integer> randomTuple = generateRandomNumber(1, 10);
            System.out.println("Random number between " + randomTuple._1() + " and " + randomTuple._2() + ": " + randomTuple._2());

            // 为了演示，我们可以创建一个包含随机数的RDD
            JavaRDD<Integer> randomNumbers = sc.parallelizeIntPairs(new int[][]{
                {1, 10},
                {5, 15},
                {7, 20}
            })
            .mapToPair(pair -> generateRandomNumber(pair._1(), pair._2()))
            .map(tuple -> tuple._2())
            .cache();

            // 打印前10个随机数
            randomNumbers.take(10).forEach(number -> System.out.println(number));

            // 释放资源
            sc.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
