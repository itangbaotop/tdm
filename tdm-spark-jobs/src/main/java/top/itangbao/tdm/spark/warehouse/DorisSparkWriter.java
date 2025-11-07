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

public class DorisSparkWriter implements WarehouseWriter {

    @Override
    public void write(Dataset<Row> df, String tableName, String jdbcUrl) {
        Properties properties = new Properties();
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
        properties.put("user", "root");
        properties.put("password", "");

        System.out.println("Writing " + df.count() + " rows to Doris: " + tableName);
        
        df.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, tableName, properties);
                
        System.out.println("Doris write completed successfully");
    }

    @Override
    public void createTableIfNotExists(Dataset<Row> df, String tableName, String jdbcUrl) {
        String[] columns = df.columns();
        
        // Doris建表SQL
        String columnDefinitions = Arrays.stream(columns)
                .map(col -> "`" + col + "` String")
                .collect(Collectors.joining(", "));
        
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "%s, " +
            "`created_time` DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") ENGINE=OLAP " +
            "DUPLICATE KEY(`%s`) " +
            "DISTRIBUTED BY HASH(`%s`) BUCKETS 1 " +
            "PROPERTIES (\"replication_num\" = \"1\")",
            tableName, columnDefinitions, columns[0], columns[0]
        );

        Properties properties = new Properties();
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
        properties.put("user", "root");
        properties.put("password", "");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = conn.createStatement()) {
            
            System.out.println("Doris creating table: " + createTableSql);
            stmt.execute(createTableSql);
            System.out.println("Doris table created: " + tableName);
            
        } catch (Exception e) {
            throw new RuntimeException("Doris table creation failed", e);
        }
    }
}