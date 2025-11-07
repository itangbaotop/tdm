package top.itangbao.tdm.spark.etl.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

//文件内容是 一段日志 然后 时序数据的标题 接着是数据 然后又是一段日志 标题 数据
public class DataFrameCleaning {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("DataFrameCleaning").master("local[*]").getOrCreate();

        // 1. 将整个文件读作一个只有一列 "value" (字符串) 的 Dataset<Row>
        Dataset<Row> dfRawText = spark.read().text("/path/to/your/messy_file.log");

        // 2. 定义 "噪音" 规则
        String logPattern = "^\\[.*";    // 正则表达式：以 [ 开头的行
        String headerPattern = "^timestamp.*"; // 正则表达式：以 timestamp 开头的行

        // 3. 过滤掉所有 "日志行" 和 "标题行"
        // 我们只想要纯数据行
        // .rlike() 是正则匹配, .not() 是取反
        Dataset<Row> dfDataLines = dfRawText.filter(
                not(
                        col("value").rlike(logPattern)
                                .or(col("value").rlike(headerPattern))
                )
        );

        // 4. 现在 dfDataLines 里全是 "1678886400,10.5,20.3" 这样的数据行
        // 我们可以安全地解析它了
        Dataset<Row> dfClean = dfDataLines.select(
                split(col("value"), ",").getItem(0).cast("long").alias("timestamp"),
                split(col("value"), ",").getItem(1).cast("double").alias("sensor_A"),
                split(col("value"), ",").getItem(2).cast("double").alias("sensor_B")
        );

        System.out.println("--- 结果 (DataFrame/Dataset API) ---");
        dfClean.show();
        dfClean.printSchema();

        spark.stop();
    }
}
