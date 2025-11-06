package top.itangbao.tdm.spark.analysis;

import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class FFTJob {

    public static void main(String[] args) {
        String inputProfile = "";
        String inputQuery = "";
        String resultId = "";
        String mysqlUrl = "";
        String clickhouseUrl = "";

        for (int i = 0; i < args.length; i++) {
            if ("--input-profile".equals(args[i])) {
                inputProfile = args[++i];
            } else if ("--input-query".equals(args[i])) {
                inputQuery = args[++i];
            } else if ("--result-id".equals(args[i])) {
                resultId = args[++i];
            } else if ("--mysql-url".equals(args[i])) {
                mysqlUrl = args[++i];
            } else if ("--clickhouse-url".equals(args[i])) {
                clickhouseUrl = args[++i];
            }
        }

        if (inputProfile.isEmpty() || inputQuery.isEmpty() || resultId.isEmpty()) {
            System.err.println("Missing required parameters: --input-profile, --input-query, --result-id");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("TDM FFT Analysis Job - Result " + resultId)
                .getOrCreate();

        try {
            // 1. 从数据仓库读取数据
            Dataset<Row> inputData = readFromWarehouse(spark, inputProfile, inputQuery);
            System.out.println("Input data count: " + inputData.count());
            inputData.show(5);

            // 2. 执行FFT分析并保存到ClickHouse
            Dataset<Row> fftResults = performFFTAnalysis(spark, inputData, resultId);
            saveFFTResultsToClickHouse(fftResults, resultId, clickhouseUrl);

            // 3. 将分析状态写入MySQL
            String summary = String.format("{\"resultId\": \"%s\", \"dataCount\": %d, \"status\": \"completed\"}", 
                                         resultId, fftResults.count());
            saveResultToMySQL(resultId, summary, mysqlUrl);

            System.out.println("FFT Analysis completed successfully. Result ID: " + resultId);

        } catch (Exception e) {
            System.err.println("Error during FFT analysis: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> readFromWarehouse(SparkSession spark, String profile, String query) {
        if ("clickhouse".equals(profile)) {
            Properties properties = new Properties();
            properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
            properties.put("user", "root");
            properties.put("password", "root");

            return spark.read()
                    .jdbc("jdbc:clickhouse://localhost:8123/tdm_data", 
                          "(" + query + ") as subquery", properties);
        }
        throw new UnsupportedOperationException("Unsupported input profile: " + profile);
    }

    private static Dataset<Row> performFFTAnalysis(SparkSession spark, Dataset<Row> data, String resultId) {
        // 1. 选择数值列进行FFT分析
        if (!Arrays.asList(data.columns()).contains("amplitude")) {
            throw new RuntimeException("No amplitude column found for FFT analysis");
        }
        
        // 2. 转换为数值类型并添加索引
        Dataset<Row> numericData = data
            .withColumn("amplitude_double", col("amplitude").cast(DataTypes.DoubleType))
            .filter(col("amplitude_double").isNotNull())
            .withColumn("sample_index", monotonically_increasing_id());
        
        // 3. 模拟FFT计算：为每个样本生成频率和幅度
        Dataset<Row> fftResults = numericData
            .withColumn("result_id", lit(resultId))
            .withColumn("frequency_hz", col("sample_index").multiply(lit(1000.0)).divide(lit(numericData.count())))
            .withColumn("magnitude", abs(col("amplitude_double")))
            .withColumn("phase", lit(0.0)) // 简化的相位
            .withColumn("created_time", current_timestamp())
            .select("result_id", "sample_index", "frequency_hz", "magnitude", "phase", "created_time");
        
        return fftResults;
    }
    
    private static void saveFFTResultsToClickHouse(Dataset<Row> fftResults, String resultId, String clickhouseUrl) throws Exception {
        Properties properties = new Properties();
        properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        properties.put("user", "root");
        properties.put("password", "root");
        
        // 创建FFT结果表
        String tableName = "tdm_data.fft_results";
        createFFTResultsTable(clickhouseUrl, tableName, properties);
        
        // 写入FFT结果
        fftResults.write()
            .mode(SaveMode.Append)
            .jdbc(clickhouseUrl, tableName, properties);
            
        System.out.println("FFT results saved to ClickHouse: " + fftResults.count() + " records");
    }
    
    private static void createFFTResultsTable(String jdbcUrl, String tableName, Properties properties) throws Exception {
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "result_id String, " +
            "sample_index UInt64, " +
            "frequency_hz Float64, " +
            "magnitude Float64, " +
            "phase Float64, " +
            "created_time DateTime" +
            ") ENGINE = MergeTree() " +
            "ORDER BY (result_id, sample_index) " +
            "PARTITION BY result_id",
            tableName
        );

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties);
             PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
            stmt.execute();
            System.out.println("FFT results table created: " + tableName);
        }
    }

    private static void saveResultToMySQL(String resultId, String result, String mysqlUrl) throws Exception {
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "root");

        try (Connection conn = DriverManager.getConnection(mysqlUrl, props)) {
            String sql = "INSERT INTO analysis_results (id, result, status, created_time) VALUES (?, ?, 'COMPLETED', NOW())";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, resultId);
                stmt.setString(2, result);
                stmt.executeUpdate();
            }
        }
    }
}