package top.itangbao.tdm.spark.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class ClickHouseSparkWriter implements WarehouseWriter {

    @Override
    public void write(Dataset<Row> df, String tableName, String jdbcUrl) {
        Properties properties = new Properties();
        properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        properties.put("user", "root");
        properties.put("password", "root");

        System.out.println("Writing " + df.count() + " rows to " + tableName);
        df.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, tableName, properties);
        System.out.println("Write completed successfully");
    }

    @Override
    public void createTableIfNotExists(Dataset<Row> df, String tableName, String jdbcUrl) {
        String[] columns = df.columns();
        
        String columnDefinitions = Arrays.stream(columns)
                .map(col -> col + " Nullable(String)")
                .collect(Collectors.joining(", "));
        
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "%s, " +
            "created_time DateTime DEFAULT now()" +
            ") ENGINE = MergeTree() " +
            "ORDER BY (%s, created_time) " +
            "PARTITION BY toYYYYMM(created_time)",
            tableName, columnDefinitions, columns[0]
        );

        Properties properties = new Properties();
        properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        properties.put("user", "root");
        properties.put("password", "root");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = conn.createStatement()) {
            
            System.out.println("ClickHouse creating table: " + createTableSql);
            stmt.execute(createTableSql);
            System.out.println("ClickHouse table created: " + tableName);
            
        } catch (Exception e) {
            throw new RuntimeException("ClickHouse table creation failed", e);
        }
    }
}