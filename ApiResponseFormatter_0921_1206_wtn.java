// 代码生成时间: 2025-09-21 12:06:02
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.HashMap;
import java.util.Map;

/**
 * ApiResponseFormatter is a utility class designed to format API responses using Spark.
 * It takes raw data and formats it into a structured JSON response.
 */
public class ApiResponseFormatter {

    private transient SparkSession sparkSession;

    /**
     * Initializes the SparkSession for the application.
     */
    public void initializeSparkSession() {
        SparkConf conf = new SparkConf().setAppName("ApiResponseFormatter").setMaster("local[*]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Formats the API response into a structured JSON.
     *
     * @param inputData dataset containing raw API data.
     * @return Dataset<Row> formatted API response as a JSON Dataset.
     */
    public Dataset<Row> formatApiResponse(Dataset<Row> inputData) {
        // Define the schema for the structured data
        StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("status", DataTypes.StringType, false),
            DataTypes.createStructField("message", DataTypes.StringType, false),
            DataTypes.createStructField("data", DataTypes.createArrayType(DataTypes.StringType), false)
        });

        try {
            // Repartition the input data if necessary to optimize performance
            inputData = inputData.repartition(1);

            // Apply the transformation to format the data into the desired JSON structure
            Dataset<Row> formattedData = inputData.selectExpr("status as status", "message as message", "collect_list(data) as data");
            formattedData = formattedData.selectExpr("status as status