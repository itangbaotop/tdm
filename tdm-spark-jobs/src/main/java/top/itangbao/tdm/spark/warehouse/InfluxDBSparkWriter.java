package top.itangbao.tdm.spark.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;

public class InfluxDBSparkWriter implements WarehouseWriter {


    @Override
    public void write(Dataset<Row> df, String tableName, String jdbcUrl) {
        String influxUrl = jdbcUrl.replace("jdbc:influxdb://", "http://");
        
        try {
            System.out.println("Writing " + df.count() + " rows to InfluxDB measurement: " + tableName);
            
            List<Row> rows = df.collectAsList();
            String[] columns = df.columns();
            
            StringBuilder lineProtocol = new StringBuilder();
            // 使用简化的measurement名称
            String measurementName = tableName.replace("tdm_data.", "").replace(".", "_");
            
            for (Row row : rows) {
                lineProtocol.append(measurementName);
                
                // 添加fields
                lineProtocol.append(" ");
                boolean firstField = true;
                for (int i = 0; i < columns.length; i++) {
                    Object value = row.get(i);
                    if (value != null && !value.toString().trim().isEmpty()) {
                        if (!firstField) lineProtocol.append(",");
                        lineProtocol.append(columns[i]).append("=\"").append(value.toString().replace("\"", "\\\"")).append("\"");
                        firstField = false;
                    }
                }
                
                // 添加时间戳（纳秒）
                lineProtocol.append(" ").append(System.currentTimeMillis() * 1000000L).append("\n");
            }
            
            // HTTP写入
            writeToInfluxDB(influxUrl, lineProtocol.toString());
            System.out.println("InfluxDB write completed successfully");
            
        } catch (Exception e) {
            throw new RuntimeException("InfluxDB write failed", e);
        }
    }

    @Override
    public void createTableIfNotExists(Dataset<Row> df, String tableName, String jdbcUrl) {
        String influxUrl = jdbcUrl.replace("jdbc:influxdb://", "http://");
        
        try {
            // 创建数据库
            createDatabase(influxUrl, "tdm_data");
            System.out.println("InfluxDB measurement ready: " + tableName);
            
        } catch (Exception e) {
            System.err.println("InfluxDB setup failed. URL: " + influxUrl);
            throw new RuntimeException("InfluxDB setup failed: " + e.getMessage(), e);
        }
    }
    
    private void createDatabase(String baseUrl, String database) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/query"))
                .POST(HttpRequest.BodyPublishers.ofString("q=CREATE DATABASE " + database))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("Database creation response: " + response.statusCode());
    }

    private void writeToInfluxDB(String baseUrl, String lineProtocol) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        System.out.println("Writing line protocol: " + lineProtocol.substring(0, Math.min(100, lineProtocol.length())));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/write?db=tdm_data"))
                .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 204) {
            throw new RuntimeException("InfluxDB write failed: " + response.statusCode() + ", response: " + response.body());
        }
    }
}