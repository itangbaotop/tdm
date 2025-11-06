package top.itangbao.tdm.spark.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface WarehouseWriter {
    
    /**
     * 写入数据到数据仓库
     * @param df 要写入的DataFrame
     * @param tableName 目标表名
     * @param jdbcUrl JDBC连接URL
     */
    void write(Dataset<Row> df, String tableName, String jdbcUrl);
    
    /**
     * 创建表（如果不存在）
     * @param df DataFrame用于推断表结构
     * @param tableName 表名
     * @param jdbcUrl JDBC连接URL
     */
    void createTableIfNotExists(Dataset<Row> df, String tableName, String jdbcUrl);
}