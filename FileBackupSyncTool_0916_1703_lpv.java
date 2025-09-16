// 代码生成时间: 2025-09-16 17:03:17
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * FileBackupSyncTool is a Java program using Spark to create file backups and synchronize directories.
 * It is designed for clear structure, error handling, documentation, best practices, maintainability, and extensibility.
 */
public class FileBackupSyncTool {

    private final JavaSparkContext sc;

    /**
     * Constructor for FileBackupSyncTool.
     * @param master Spark master URL.
     */
    public FileBackupSyncTool(String master) {
        this.sc = new JavaSparkContext(master, "FileBackupSyncTool");
    }

    /**
     * Backups the files by compressing them into a zip file.
     * @param sourcePath The source directory path for backup.
     * @param targetPath The target directory path for storing the backup.
     */
    public void backupFiles(String sourcePath, String targetPath) {
        try {
            JavaRDD<String> filePaths = sc.textFile(sourcePath);
            filePaths.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> filePaths) throws Exception {
                    if (!filePaths.hasNext()) return;

                    String zipFileName = targetPath + "/backup.zip";
                    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFileName));

                    while (filePaths.hasNext()) {
                        String filePath = filePaths.next();
                        File fileToZip = new File(filePath);
                        if (fileToZip.exists() && fileToZip.isFile()) {
                            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                            zos.putNextEntry(zipEntry);

                            FileInputStream fis = new FileInputStream(fileToZip);
                            byte[] bytes = new byte[1024];
                            int length;
                            while ((length = fis.read(bytes)) >= 0) {
                                zos.write(bytes, 0, length);
                            }
                            fis.close();
                        }
                    }

                    zos.close();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Synchronizes the files between two directories.
     * @param sourcePath The source directory path.
     * @param targetPath The target directory path for synchronization.
     */
    public void syncFiles(String sourcePath, String targetPath) {
        try {
            JavaRDD<String> sourceFiles = sc.textFile(sourcePath);
            JavaRDD<String> targetFiles = sc.textFile(targetPath);

            // Remove files in target that are not in source
            targetFiles.foreach(new VoidFunction<String>() {
                @Override
                public void call(String targetFile) throws Exception {
                    if (!sourceFiles.collect().contains(targetFile)) {
                        new File(targetFile).delete();
                    }
                }
            });

            // Copy new or updated files from source to target
            sourceFiles.foreach(new VoidFunction<String>() {
                @Override
                public void call(String sourceFile) throws Exception {
                    if (!new File(targetPath, sourceFile).exists() ||
                            new File(sourceFile).lastModified() != new File(targetPath, sourceFile).lastModified()) {
                        Files.copy(Paths.get(sourceFile), Paths.get(targetPath, sourceFile),
                                StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Stops the Spark context.
     */
    public void stop() {
        sc.stop();
    }

    /**
     * Main method to run the program.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: FileBackupSyncTool <master> <sourcePath> <targetPath> <operation>");
            System.exit(1);
        }

        String master = args[0];
        String sourcePath = args[1];
        String targetPath = args[2];
        String operation = args[3];

        FileBackupSyncTool tool = new FileBackupSyncTool(master);
        try {
            switch (operation) {
                case "backup":
                    tool.backupFiles(sourcePath, targetPath);
                    break;
                case "sync":
                    tool.syncFiles(sourcePath, targetPath);
                    break;
                default:
                    System.err.println("Invalid operation. Please use 'backup' or 'sync'.");
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tool.stop();
        }
    }
}
