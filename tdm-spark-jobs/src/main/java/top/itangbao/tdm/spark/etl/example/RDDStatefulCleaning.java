package top.itangbao.tdm.spark.etl.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

//文件内容是 一段日志 然后 时序数据的标题 接着是数据 然后又是一段日志 标题 数据
public class RDDStatefulCleaning {


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("RDDStatefulCleaning").master("local[*]").getOrCreate();

        // 1. 以 RDD 的方式读取文本文件，得到一个 Dataset<String>
        Dataset<String> dsRawText = spark.read().textFile("/path/to/your/messy_file.log");

        // 2. 转换为 JavaRDD<String>
        JavaRDD<String> rddRawText = dsRawText.javaRDD();

        // 3. 在每个分区上运行我们的 "状态机"
        // mapPartitions 接收一个迭代器，并返回一个新的迭代器
        JavaRDD<TimeData> rddCleanData = rddRawText.mapPartitions(iterator -> {

            // 这是在每个 Executor 上运行的代码
            List<TimeData> partitionData = new ArrayList<>();
            boolean inDataBlock = false; // 我们的 "状态"

            while (iterator.hasNext()) {
                String line = iterator.next().trim();

                // 规则 1: 识别日志行 (假设以 [ 开头)
                if (line.startsWith("[")) {
                    inDataBlock = false; // 遇到日志，退出数据状态
                    continue;
                }

                // 规则 2: 识别标题行 (假设以 'timestamp' 开头)
                if (line.startsWith("timestamp")) {
                    inDataBlock = true;  // 遇到标题，进入数据状态
                    continue; // 不输出标题行本身
                }

                // 规则 3: 如果我们在数据状态，这就是我们要的数据
                if (inDataBlock) {
                    try {
                        String[] parts = line.split(",");
                        TimeData data = new TimeData();
                        data.setTimestamp(Long.parseLong(parts[0]));
                        data.setSensor_A(Double.parseDouble(parts[1]));
                        data.setSensor_B(Double.parseDouble(parts[2]));
                        partitionData.add(data);
                    } catch (Exception e) {
                        // 容错：跳过解析失败的脏数据
                    }
                }
            }
            // mapPartitions 必须返回一个迭代器
            return partitionData.iterator();
        });

        // 4. 将干净的 RDD (包含 POJO) 转换回 DataFrame
        Dataset<Row> dfClean = spark.createDataFrame(rddCleanData, TimeData.class);

        System.out.println("--- 结果 (RDD/Core API) ---");
        dfClean.show();
        dfClean.printSchema();

        spark.stop();
    }

    static class TimeData {
        private long timestamp;
        private double sensor_A;
        private double sensor_B;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getSensor_A() {
            return sensor_A;
        }

        public void setSensor_A(double sensor_A) {
            this.sensor_A = sensor_A;
        }

        public double getSensor_B() {
            return sensor_B;
        }

        public void setSensor_B(double sensor_B) {
            this.sensor_B = sensor_B;
        }
    }
}
