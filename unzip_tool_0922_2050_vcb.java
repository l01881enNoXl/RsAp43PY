// 代码生成时间: 2025-09-22 20:50:00
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class UnzipTool {
    
    // Function to unzip a file
    public static void unzip(String zipFilePath, String destDirectory) {
        try {
            // Check if destination directory exists, if not create it
            File destDir = new File(destDirectory);
            if (!destDir.exists()) {
                destDir.mkdir();
            }

            // Open ZipInputStream and read zip file
            ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
            ZipEntry entry = zipIn.getNextEntry();

            // Loop through the zip file entries and extract each one
            while (entry != null) {
                String filePath = destDirectory + File.separator + entry.getName();
                if (!entry.isDirectory()) {
                    // If file, extract it
                    extractFile(zipIn, filePath);
                } else {
                    // If directory, make the directory
                    File dir = new File(filePath);
                    dir.mkdir();
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
            zipIn.close();
            System.out.println("Successfully extracted the zip file into directory: " + destDirectory);
        } catch (IOException e) {
            System.err.println("Error occurred while unzipping the file: " + e.getMessage());
        }
    }

    // Helper function to extract a file from zip input stream
    private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
            byte[] bytesIn = new byte[4096];
            int read = 0;
            while ((read = zipIn.read(bytesIn)) != -1) {
                bos.write(bytesIn, 0, read);
            }
        }
    }

    // Main function to run the program
    public static void main(String[] args) {
        // Spark configuration and context setup for distributed processing if needed
        SparkConf conf = new SparkConf().setAppName("UnzipTool");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Check if enough arguments are provided
        if (args.length < 2) {
            System.err.println("Usage: UnzipTool <zip file path> <destination directory>");
            System.exit(1);
        }

        // Unzip the file with provided arguments
        String zipFilePath = args[0];
        String destDirectory = args[1];
        unzip(zipFilePath, destDirectory);
    }
}