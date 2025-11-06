package top.itangbao.tdm.spark.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CleanerJob {

    public static void main(String[] args) {
        // 1. 解析命令行参数
        // 我们期望的参数: --input-uri s3a://.../file.dat --task-id 123
        String inputUri = "";
        String taskId = "";
        for (int i = 0; i < args.length; i++) {
            if ("--input-uri".equals(args[i])) {
                inputUri = args[++i];
            } else if ("--task-id".equals(args[i])) {
                taskId = args[++i];
            }
        }

        if (inputUri.isEmpty()) {
            System.err.println("Missing --input-uri parameter");
            System.exit(1);
        }

        System.out.printf("Starting Spark ETL Job for Task ID: %s%n", taskId);
        System.out.printf("Reading from: %s%n", inputUri);

        // 2.【关键】配置 SparkSession 以连接 MinIO
        SparkSession spark = SparkSession.builder()
                .appName("TDM ETL Cleaner Job - Task " + taskId)
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        try {
            // 3. (MVP 逻辑) 读取文件
            // 我们假设原始文件是纯文本
            Dataset<Row> rawData = spark.read().text(inputUri);

            // 4. (MVP 逻辑) 打印前 10 行
            System.out.println("====== Data from file (Top 10 lines) ======");
            rawData.show(10, false);
            System.out.println("==========================================");

            // TODO (阶段 4):
            // 在这里添加真正的清洗逻辑 (e.g., rawData.filter(...))
            // 并将结果写入 ClickHouse / Doris

            System.out.println("Spark Job completed successfully.");

        } catch (Exception e) {
            System.err.println("Error during Spark job: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 5. 停止 SparkSession
            spark.stop();
        }
    }
}