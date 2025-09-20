// 代码生成时间: 2025-09-20 11:47:48
 * 作者：[你的姓名]
 * 日期：[创建日期]
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ImageResizeBatchProcessor {

    // 构造Spark上下文
    private transient SparkSession spark;

    public ImageResizeBatchProcessor(String appName) {
        this.spark = SparkSession
            .builder()
            .appName(appName)
            .master("local[*]")
            .getOrCreate();
    }

    // 处理单个图片尺寸调整
    private BufferedImage resizeImage(BufferedImage originalImage, int targetWidth, int targetHeight) {
        try {
            // 调整图片尺寸
            BufferedImage resizedImage = new BufferedImage(targetWidth, targetHeight, originalImage.getType());
            resizedImage.getGraphics().drawImage(originalImage.getScaledInstance(targetWidth, targetHeight, java.awt.Image.SCALE_SMOOTH), 0, 0, null);
            return resizedImage;
        } catch (Exception e) {
            throw new RuntimeException("Failed to resize image", e);
        }
    }

    // 批量处理图片尺寸调整
    public void processBatch(List<String> imageFilePaths, int targetWidth, int targetHeight, String outputDirectory) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> imageFilePathsRDD = sc.parallelize(imageFilePaths);

        imageFilePathsRDD.foreach(filePath -> {
            try {
                File imageFile = new File(filePath);
                BufferedImage originalImage = ImageIO.read(imageFile);
                BufferedImage resizedImage = resizeImage(originalImage, targetWidth, targetHeight);

                // 确保输出目录存在
                File outputDirectoryFile = new File(outputDirectory);
                if (!outputDirectoryFile.exists()) {
                    outputDirectoryFile.mkdirs();
                }

                // 保存调整后的图片
                String outputFilePath = outputDirectory + "/" + UUID.randomUUID().toString() + imageFile.getName();
                File outputFile = new File(outputFilePath);
                ImageIO.write(resizedImage, "png", outputFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to process image file: " + filePath, e);
            }
        });
    }

    // 关闭Spark上下文
    public void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        try {
            // 检查输入参数
            if (args.length < 4) {
                System.err.println("Usage: ImageResizeBatchProcessor <imageFilePaths> <targetWidth> <targetHeight> <outputDirectory>");
                System.exit(1);
            }

            // 初始化图片批量处理器
            ImageResizeBatchProcessor processor = new ImageResizeBatchProcessor("ImageResizeBatchProcessor");

            // 解析输入参数
            List<String> imageFilePaths = new ArrayList<>();
            for (int i = 0; i < args.length - 3; i++) {
                imageFilePaths.add(args[i]);
            }
            int targetWidth = Integer.parseInt(args[args.length - 3]);
            int targetHeight = Integer.parseInt(args[args.length - 2]);
            String outputDirectory = args[args.length - 1];

            // 执行批量处理
            processor.processBatch(imageFilePaths, targetWidth, targetHeight, outputDirectory);

            // 停止Spark上下文
            processor.stop();
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}