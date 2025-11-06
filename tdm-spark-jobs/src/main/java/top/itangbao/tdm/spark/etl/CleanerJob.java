package top.itangbao.tdm.spark.etl;

import org.apache.spark.sql.*;
// 导入 Spark SQL 函数

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


public class CleanerJob {

    public static void main(String[] args) {
        // 1. 解析命令行参数
        String inputUri = "";
        String taskId = "";
        String warehouseHost = "";
        String warehousePort = "";
        String warehouseTable = "";

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
            
            // ClickHouse JDBC 属性
            Properties properties = new Properties();
            properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
            properties.put("user", "root");
            properties.put("password", "root");
            
            // 自动创建表（基于第一行标题）
            createTableFromCsvHeader(clickhouseJdbcUrl, warehouseTable, rawCsvData, properties);
            
            // 数据清洗（过滤空值）
            Dataset<Row> cleanedData = rawCsvData.na().drop();

            System.out.println("====== Cleaned data (Top 5) ======");
            cleanedData.show(5, false);

            // 5. 【【核心：L (加载/入库)】】
            //    将数据写入 ClickHouse





            cleanedData
                    .write()
                    .mode(SaveMode.Append) // 我们总是追加数据
                    .jdbc(clickhouseJdbcUrl, warehouseTable, properties);

            System.out.println("Spark Job completed successfully. Data written to ClickHouse.");

        } catch (Exception e) {
            System.err.println("Error during Spark job: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 停止 SparkSession
            spark.stop();
        }
    }

    private static void createTableFromCsvHeader(String jdbcUrl, String tableName, Dataset<Row> csvData, Properties properties) {
        // 获取CSV的列名
        String[] columns = csvData.columns();
        
        // 构建建表SQL（所有字段都使用String类型）
        String columnDefinitions = Arrays.stream(columns)
                .map(col -> col + " String")
                .collect(Collectors.joining(", "));
        
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "%s, " +
            "created_time DateTime DEFAULT now()" +
            ") ENGINE = MergeTree() " +
            "ORDER BY (%s, created_time) " +
            "PARTITION BY toYYYYMM(created_time)",
            tableName, columnDefinitions, columns[0] // 使用第一列作为主键
        );

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = conn.createStatement()) {
            
            System.out.println("Auto-creating table with SQL: " + createTableSql);
            stmt.execute(createTableSql);
            System.out.println("Table created successfully: " + tableName);
            
        } catch (Exception e) {
            System.err.println("Failed to create table: " + e.getMessage());
            throw new RuntimeException("Table creation failed", e);
        }
    }
}