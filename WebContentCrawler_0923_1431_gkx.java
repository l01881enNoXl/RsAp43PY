// 代码生成时间: 2025-09-23 14:31:33
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class WebContentCrawler {
    private SparkSession spark;
    private JavaSparkContext sc;
    private Broadcast<Map<String, String>> headers;

    public WebContentCrawler(SparkSession spark) {
        this.spark = spark;
        this.sc = new JavaSparkContext(spark.sparkContext());
        // Define default headers
        Map<String, String> defaultHeaders = new HashMap<>();
        defaultHeaders.put("User-Agent", "Mozilla/5.0");
        this.headers = sc.broadcast(defaultHeaders);
    }

    /**
     * Fetches the content of a URL and returns it as a string.
     *
     * @param urlString The URL to fetch content from.
     * @return The content of the URL as a string.
     */
    private String fetchContent(String urlString) {
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            Map<String, String> headers = this.headers.value();
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                connection.setRequestProperty(entry.getKey(), entry.getValue());
            }
            connection.connect();
            try (Scanner scanner = new Scanner(url.openStream(), StandardCharsets.UTF_8.name())) {
                scanner.useDelimiter("\A");
                return scanner.hasNext() ? scanner.next() : "";
            }
        } catch (IOException e) {
            System.err.println("Error fetching content from URL: " + urlString);
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Crawls web content for a list of URLs and returns the content as an RDD of Strings.
     *
     * @param urls A list of URLs to crawl.
     * @return An RDD containing the content of each URL.
     */
    public JavaRDD<String> crawlWebContent(Iterable<String> urls) {
        return sc.parallelize(urls).map(url -> fetchContent(url));
    }

    /**
     * Main method to run the web content crawler.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WebContentCrawler");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        WebContentCrawler crawler = new WebContentCrawler(spark);

        // Example usage: crawl content for a list of URLs
        Iterable<String> urls = List.of(
                "http://example.com",
                "http://example.org"
        );

        JavaRDD<String> contentRDD = crawler.crawlWebContent(urls);
        contentRDD.foreach(content -> System.out.println(content));
    }
}
