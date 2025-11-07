package top.itangbao.tdm.spark.etl;

import org.apache.spark.sql.*;
// 导入 Spark SQL 函数

import java.util.Properties;
import top.itangbao.tdm.spark.warehouse.WarehouseWriter;
import top.itangbao.tdm.spark.warehouse.WarehouseWriterFactory;

import static org.apache.spark.sql.functions.*;


public class CleanerJob {

    public static void main(String[] args) {
        // 1. 解析命令行参数
        String inputUri = "";
        String taskId = "";
        String warehouseHost = "";
        String warehousePort = "";
        String warehouseTable = "";
        String warehouseProfile = "clickhouse"; // 默认使用ClickHouse

        for (int i = 0; i < args.length; i++) {
            if ("--input-uri".equals(args[i])) {
                inputUri = args[++i];
            } else if ("--task-id".equals(args[i])) {
                taskId = args[++i];
            } else if ("--warehouse-host".equals(args[i])) { // 【新增】
                warehouseHost = args[++i];
            } else if ("--warehouse-port".equals(args[i])) { // 【新增】
                warehousePort = args[++i];
            } else if ("--warehouse-table".equals(args[i])) { // 【新增】
                warehouseTable = args[++i];
            } else if ("--warehouse-profile".equals(args[i])) { // 【新增】
                warehouseProfile = args[++i];
            }
        }

        if (inputUri.isEmpty() || warehouseTable.isEmpty() || warehouseHost.isEmpty()) {
            System.err.println("Missing required parameters: --input-uri, --warehouse-host, --warehouse-port, --warehouse-table");
            System.exit(1);
        }

        String clickhouseJdbcUrl = String.format("jdbc:clickhouse://%s:%s/tdm_data", warehouseHost, warehousePort);

        System.out.printf("Starting Spark ETL Job for Task ID: %s%n", taskId);
        System.out.printf("Reading from: %s%n", inputUri);
        System.out.printf("Writing to ClickHouse table: %s at %s%n", warehouseTable, clickhouseJdbcUrl);


        // 2. 配置 SparkSession (S3A 配置保持不变)
        SparkSession spark = SparkSession.builder()
                .appName("TDM ETL Cleaner Job - Task " + taskId)
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        try {
            // 3. (读取) 读取CSV文件 - 移到转换部分

            // 4. 【【核心：T (转换/清洗)】】
            // 读取CSV文件并自动创建表
            Dataset<Row> rawCsvData = spark.read()
                    .option("header", "true")
                    .csv(inputUri);
            
            // 数据清洗（只过滤全部为空的行）
            System.out.println("Raw data count: " + rawCsvData.count());
            Dataset<Row> cleanedData = rawCsvData.na().drop("all"); // 只删除全部列都为空的行
            System.out.println("Cleaned data count: " + cleanedData.count());

            System.out.println("====== Cleaned data (Top 5) ======");
            cleanedData.show(5, false);
            
            if (cleanedData.count() == 0) {
                System.err.println("Warning: No data to write after cleaning!");
                return;
            }

            // 5. 【【核心：L (加载/入库)】】
            // 使用工厂模式选择数据仓库writer
            WarehouseWriter writer = WarehouseWriterFactory.createWriter(warehouseProfile);
            
            // 根据仓库类型构建正确的URL
            String warehouseUrl;
            if ("influxdb".equals(warehouseProfile)) {
                warehouseUrl = "jdbc:influxdb://" + warehouseHost + ":8086";
            } else if ("doris".equals(warehouseProfile)) {
                warehouseUrl = "jdbc:mysql://" + warehouseHost + ":9030/tdm_data";
            } else {
                warehouseUrl = clickhouseJdbcUrl;
            }
            
            // 自动创建表
            writer.createTableIfNotExists(rawCsvData, warehouseTable, warehouseUrl);
            
            // 写入数据
            writer.write(cleanedData, warehouseTable, warehouseUrl);

            System.out.println("Spark Job completed successfully. Data written to ClickHouse.");

        } catch (Exception e) {
            System.err.println("Error during Spark job: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 停止 SparkSession
            spark.stop();
        }
    }


}