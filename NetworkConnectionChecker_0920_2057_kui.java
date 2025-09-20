// 代码生成时间: 2025-09-20 20:57:49
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NetworkConnectionChecker {
    // 函数：检查网络连接状态
    public static boolean checkConnection(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            // 如果能够成功连接，返回true
            return socket.isConnected();
        } catch (UnknownHostException e) {
            // 捕获主机未知异常
            System.err.println("UnknownHostException: " + e.getMessage());
        } catch (Exception e) {
            // 捕获其他异常
            System.err.println("Exception: " + e.getMessage());
        }
        // 如果发生异常，返回false
        return false;
    }

    public static void main(String[] args) {
        // Spark配置
        SparkConf conf = new SparkConf()
            .setAppName("NetworkConnectionChecker")
            .setMaster("local[*]");
        // 初始化Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 定义要检查的主机和端口列表
        List<String> hosts = Arrays.asList("www.google.com", "www.example.com");
        int port = 80; // HTTP端口

        // 使用Spark并行检查连接状态
        JavaRDD<String> connectionStatus = sc.parallelize(hosts)
            .map(host -> host + " is connected: " + checkConnection(host, port));

        // 收集并打印结果
        connectionStatus.collect().forEach(System.out::println);

        // 关闭Spark上下文
        sc.close();
    }
}
