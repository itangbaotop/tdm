package top.itangbao.tdm.spark.analysis.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.DefaultParamsWritable;
import org.apache.spark.ml.util.Identifiable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.util.Arrays;
import java.util.List;

// 导入 Spark SQL 静态函数，如 col(), min(), max()
import static org.apache.spark.sql.functions.*;

/**
 * 演示在 Spark 中实现自定义算法的三种方式
 *
 * 这个算法的逻辑是：
 *
 * 找到某一列的最小值 (min) 和最大值 (max)。
 *
 * 对该列中的每一个值 x，应用公式 scaled_x = (x - min) / (max - min)，将所有值缩放到 [0, 1] 区间。
 *
 * ----------------------------------------------------------------------------------------------------
 * 在 Spark 中，“常用算法”主要可以分为以下几个大家族，其中机器学习库 (MLlib) 是重中之重。
 *
 * 1. 🤖 机器学习 (MLlib) - Spark 的看家本领
 * 这是 Spark 中最大、最常用的算法库。它几乎涵盖了所有主流的监督和非监督学习方法。
 *
 * A. 分类 (Classification)
 * 当你的目标是预测一个类别时（比如：是不是垃圾邮件？客户是否会流失？）。
 *
 * 逻辑回归 (Logistic Regression): 最基础也是最常用的二分类算法，速度快，易于解释。
 *
 * 决策树 (Decision Trees): 非常直观，像一个流程图，可解释性强。
 *
 * 随机森林 (Random Forest): “森林”由很多“决策树”组成。它通过投票来提高准确性，是目前最好用的“开箱即用”算法之一。
 *
 * GBTs (Gradient-Boosted Trees, 梯度提升树): 另一种基于树的强大算法，通常在各种数据竞赛中表现优异，准确率极高，但训练较慢。
 *
 * 朴素贝叶斯 (Naive Bayes): 尤其擅长文本分类（比如垃圾邮件过滤）。
 *
 * B. 回归 (Regression)
 * 当你的目标是预测一个连续的数值时（比如：预测房价、预测销量）。
 *
 * 线性回归 (Linear Regression): 最基础的回归算法，用于寻找变量间的线性关系。
 *
 * 决策树 / 随机森林 / GBTs: 没错，这些基于树的模型同样可以用于回归任务，表现也非常出色。
 *
 * 广义线性回归 (Generalized Linear Regression, GLR): 线性回归的扩展，允许你处理不同类型的数据分布（比如泊松回归用于计数数据）。
 *
 * C. 聚类 (Clustering)
 * 当你的数据没有“答案”（标签）时，你想让算法自动找出数据中的“群组”。
 *
 * K-Means (K均值): 最著名、最简单的聚类算法。你需要预先指定要找出的群组数量 (K)。
 *
 * GMM (Gaussian Mixture Model, 高斯混合模型): K-Means 的升级版。它更灵活，允许数据点“软性”地属于多个聚类。
 *
 * LDA (Latent Dirichlet Allocation): 主要用于主题模型 (Topic Modeling)。它可以从一堆文档中自动发现“主题”（比如把新闻自动分为“体育”、“财经”、“科技”）。
 *
 * D. 推荐 (Recommendation)
 * 用于“猜你喜欢”的场景，比如电商、视频网站。
 *
 * ALS (Alternating Least Squares, 交替最小二乘法): 这是 Spark MLlib 中用于构建协同过滤 (Collaborative Filtering) 推荐系统的核心算法。
 *
 * 2. 🔬 特征工程与统计 (Feature & Statistics)
 * 这一类和你的 FFT 很像，它们本身不是“预测模型”，而是**“准备数据”和“理解数据”**的算法。
 *
 * PCA (Principal Component Analysis, 主成分分析): 非常强大的降维算法。当你的数据有几百上千列（特征）时，PCA 可以帮你把它们压缩到少数几个“主成分”上，同时保留大部分信息。
 *
 * TF-IDF (Term Frequency-Inverse Document Frequency): 文本分析的基石。用于衡量一个词在文档中的重要性。
 *
 * Word2Vec: 一种“词嵌入”算法，它能把单词转换成数学向量，让机器理解词与词之间的语义关系（比如 "国王" - "男人" + "女人" ≈ "王后"）。
 *
 * 相关性分析 (Correlation): 计算不同特征之间的皮尔逊 (Pearson) 或斯皮尔曼 (Spearman) 相关系数。这是数据探索的第一步，帮你理解哪些变量是相关的。
 *
 * 3. 🕸️ 图计算 (GraphX)
 * 如果你的数据是“网络”结构（比如社交网络的用户关系、物流的节点网络），Spark 有一个专门的库 GraphX。
 *
 * PageRank: 谷歌早期的核心算法，用于评估网页的重要性（节点的重要性）。
 *
 * Connected Components (连通分量): 找出网络中相互连接的“群组”。
 *
 * Triangle Counting (三角形计数): 分析网络的紧密程度，常用于社区发现。
 *
 * 4. 🛒 模式挖掘 (Pattern Mining)
 * FP-Growth: 用于频繁项集挖掘。最经典的例子是“啤酒与尿布”的关联规则 (Market Basket Analysis)，找出哪些东西经常被一起购买。
 */
public class CustomSparkAlgorithms {

    /**
     * 方式一：组合 DataFrame API
     * 理念：使用高级函数式 API。代码简洁、可读性强，并能享受 Catalyst 优化。
     * 适用：绝大多数特征工程和业务逻辑。
     */
    public static class Method1_DataFrame {

        public static Dataset<Row> scale(Dataset<Row> df, String inputCol, String outputCol) {
            System.out.println("--- 运行方式一：DataFrame API ---");

            // 1. 使用 agg() 一次性计算 min 和 max
            // .first() 会返回一个 Row，格式为 [min_val, max_val]
            Row minMaxRow = df.agg(min(inputCol), max(inputCol)).first();
            double minVal = minMaxRow.getDouble(0);
            double maxVal = minMaxRow.getDouble(1);
            double range = maxVal - minVal;

            // 检查除零错误
            if (range == 0.0) {
                // 如果 max == min，所有值都一样，可以将它们缩放为 0 或 0.5
                return df.withColumn(outputCol, lit(0.5));
            }

            // 2. 使用 withColumn() 和列操作来应用公式
            Dataset<Row> scaledDf = df.withColumn(outputCol,
                    (col(inputCol).minus(minVal)).divide(range)
            );

            return scaledDf;
        }
    }


    /**
     * 方式二：实现 MLlib Pipeline 接口
     * 理念：遵循 Spark MLlib 的标准，创建可重用的、可加入 Pipeline 的组件。
     * 适用：实现标准的、可重用的机器学习步骤。
     *
     * 注意：这是一个 Estimator (估计器)，它 .fit() 数据来学习 min/max，
     * 然后返回一个 Transformer (转换器) 或 Model (模型) 来 .transform() 数据。
     */
    public static class Method2_MLlib {

        // --- 估计器 (Estimator)：用于 "学习" ---
        // 它负责 .fit()，计算 min 和 max
        public static class MyMinMaxScaler extends Estimator<MyMinMaxScalerModel> implements DefaultParamsWritable {

            private final String uid;
            private final Param<String> inputCol;
            private final Param<String> outputCol;

            public MyMinMaxScaler(String uid) {
                this.uid = uid;
                this.inputCol = new Param<>(this, "inputCol", "The input column");
                this.outputCol = new Param<>(this, "outputCol", "The output column");
            }
            public MyMinMaxScaler() { this(Identifiable.randomUID("myMinMaxScaler")); }

            // Setters
            public MyMinMaxScaler setInputCol(String value) { set(inputCol, value); return this; }
            public MyMinMaxScaler setOutputCol(String value) { set(outputCol, value); return this; }

            // Getters
            public Option<String> getInputCol() { return get(inputCol); }
            public Option<String> getOutputCol() { return get(outputCol); }

            @Override
            public MyMinMaxScalerModel fit(Dataset<?> dataset) {
                // 1. 从训练数据中 "学习" min 和 max
                String colName = getInputCol().get();
                Row minMaxRow = dataset.agg(min(colName), max(colName)).first();
                double minVal = minMaxRow.getDouble(0);
                double maxVal = minMaxRow.getDouble(1);

                // 2. 返回一个 "模型" (Transformer)，该模型存储了学习到的 min/max
                return new MyMinMaxScalerModel(this.uid, minVal, maxVal)
                        .setInputCol(colName)
                        .setOutputCol(getOutputCol().get());
            }

            @Override
            public StructType transformSchema(StructType schema) {
                // 检查输入列是否存在且为数值类型
                String inCol = getInputCol().get();
                if (!schema.simpleString().contains(inCol)) {
                    throw new IllegalArgumentException("Input column " + inCol + " does not exist.");
                }
                // 添加输出列
                return schema.add(getOutputCol().get(), DataTypes.DoubleType, false);
            }

            @Override
            public MyMinMaxScaler copy(ParamMap extra) {
                return defaultCopy(extra);
            }

            @Override
            public String uid() { return this.uid; }
        }

        // --- 模型 (Model / Transformer)：用于 "转换" ---
        // 它存储 min/max 并应用 .transform()
        public static class MyMinMaxScalerModel extends Model<MyMinMaxScalerModel> implements DefaultParamsWritable {

            private final String uid;
            private final double originalMin;
            private final double originalMax;
            private final Param<String> inputCol;
            private final Param<String> outputCol;

            public MyMinMaxScalerModel(String uid, double min, double max) {
                this.uid = uid;
                this.originalMin = min;
                this.originalMax = max;
                this.inputCol = new Param<>(this, "inputCol", "The input column");
                this.outputCol = new Param<>(this, "outputCol", "The output column");
            }

            // Setters
            public MyMinMaxScalerModel setInputCol(String value) { set(inputCol, value); return this; }
            public MyMinMaxScalerModel setOutputCol(String value) { set(outputCol, value); return this; }

            @Override
            public Dataset<Row> transform(Dataset<?> dataset) {
                double range = originalMax - originalMin;
                if (range == 0.0) {
                    return dataset.withColumn(get(outputCol).get(), lit(0.5));
                }

                return dataset.withColumn(get(outputCol).get(),
                        (col(get(inputCol).get()).minus(originalMin)).divide(range)
                );
            }

            @Override
            public StructType transformSchema(StructType schema) {
                return schema.add(get(outputCol).get(), DataTypes.DoubleType, false);
            }

            @Override
            public MyMinMaxScalerModel copy(ParamMap extra) {
                return defaultCopy(extra);
            }
            @Override
            public String uid() { return this.uid; }
        }
    }


    /**
     * 方式三：使用 RDD API
     * 理念：最底层的、手动的、指令式的控制。你必须自己管理所有事情。
     * 适用：无法用 DataFrame 表达的、复杂的迭代算法或需要精细分区控制的算法。
     */
    public static class Method3_RDD {

        public static Dataset<Row> scale(Dataset<Row> df, String inputCol) {
            System.out.println("--- 运行方式三：RDD API ---");

            JavaSparkContext jsc = new JavaSparkContext(df.sparkSession().sparkContext());
            int colIndex = df.schema().fieldIndex(inputCol);

            // 1. 将 DataFrame 转换为 RDD
            JavaRDD<Row> rdd = df.javaRDD();

            // 2. 在一次传递中计算 min 和 max
            // (这是比先 mapToDouble 再调用 .min() 和 .max() 更高效的 RDD 方式)
            // (a) 将 RDD 映射为 (value, value)
            // (b) 使用 reduce 找到 (min, max)
            scala.Tuple2<Double, Double> minMax = rdd.map(row -> {
                double val = row.getDouble(colIndex);
                return new scala.Tuple2<>(val, val);
            }).reduce((tuple1, tuple2) -> {
                double min = Math.min(tuple1._1, tuple2._1);
                double max = Math.max(tuple1._2, tuple2._2);
                return new scala.Tuple2<>(min, max);
            });

            double minVal = minMax._1;
            double maxVal = minMax._2;
            double range = maxVal - minVal;

            // 3. 将 min 和 range 广播 (Broadcast) 到所有 Executor
            // 这是 RDD 编程的最佳实践，避免在 map 闭包中序列化整个任务
            Broadcast<Double> bMin = jsc.broadcast(minVal);
            Broadcast<Double> bRange = jsc.broadcast(range);

            // 4. 再次遍历 RDD (map) 以应用缩放
            JavaRDD<Row> scaledRDD = rdd.map(row -> {
                double originalVal = row.getDouble(colIndex);
                double scaledVal = 0.5; // 默认值 (如果 range == 0)

                double bRangeVal = bRange.value();
                if (bRangeVal != 0.0) {
                    scaledVal = (originalVal - bMin.value()) / bRangeVal;
                }
                // 注意：你需要手动重建 Row
                return RowFactory.create(originalVal, scaledVal);
            });

            // 5. 将 RDD 转换回 DataFrame，你需要手动定义 Schema
            StructType newSchema = new StructType(new StructField[]{
                    new StructField(inputCol, DataTypes.DoubleType, false, null),
                    new StructField("scaled", DataTypes.DoubleType, false, null)
            });

            return df.sparkSession().createDataFrame(scaledRDD, newSchema);
        }
    }


    /**
     * -----------------------------------------------------------------
     * 运行所有示例的 Main 方法
     * -----------------------------------------------------------------
     */
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("CustomSparkAlgorithmsDemo")
                .master("local[*]")
                .getOrCreate();

        // 1. 准备示例数据
        List<Row> data = Arrays.asList(
                RowFactory.create(1.0),
                RowFactory.create(2.0),
                RowFactory.create(3.0),
                RowFactory.create(4.0),
                RowFactory.create(5.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("feature", DataTypes.DoubleType, false, null)
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);
        System.out.println("原始数据:");
        df.show();

        // --- 执行方式一 ---
        Dataset<Row> df1 = Method1_DataFrame.scale(df, "feature", "scaled");
        df1.show();

        // --- 执行方式二 ---
        System.out.println("--- 运行方式二：MLlib API ---");
        Method2_MLlib.MyMinMaxScaler scaler = new Method2_MLlib.MyMinMaxScaler()
                .setInputCol("feature")
                .setOutputCol("scaled");

        // 训练 (fit)：计算 min/max
        Method2_MLlib.MyMinMaxScalerModel model = scaler.fit(df);

        // 转换 (transform)：应用公式
        Dataset<Row> df2 = model.transform(df);
        df2.show();

        // --- 执行方式三 ---
        Dataset<Row> df3 = Method3_RDD.scale(df, "feature");
        df3.show();

        spark.stop();
    }
}
