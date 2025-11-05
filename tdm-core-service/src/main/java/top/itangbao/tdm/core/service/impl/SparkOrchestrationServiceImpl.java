package top.itangbao.tdm.core.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import top.itangbao.tdm.core.service.SparkOrchestrationService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class SparkOrchestrationServiceImpl implements SparkOrchestrationService {

    @Value("${spark.submit.path}")
    private String sparkSubmitPath;

    @Value("${spark.master.url}")
    private String sparkMasterUrl;

    @Value("${spark.job.jar-path}")
    private String sparkJobJarPath;

    @Override
    public void submitEtlJob(Long taskId, String inputUri) {
        log.info("Submitting Spark ETL job for Task ID: {}", taskId);

        // 1. 构建 Fat JAR 的绝对路径
        // (假设 `tdm-core-service` 和 `tdm-spark-jobs` 在同一父目录下)
        String jobJarAbsolutePath = new File(sparkJobJarPath).getAbsolutePath();
        log.info("Spark Job JAR path: {}", jobJarAbsolutePath);

        // 2. 构建 spark-submit 命令
        List<String> command = new ArrayList<>();
        command.add(sparkSubmitPath);
        command.add("--class");
        command.add("top.itangbao.tdm.spark.etl.CleanerJob"); // 我们 Spark 任务的主类
        command.add("--master");
        command.add(sparkMasterUrl);
        command.add(jobJarAbsolutePath); // JAR 包

        // 3. 添加 Spark 任务所需的参数
        command.add("--input-uri");
        command.add(inputUri);
        command.add("--task-id");
        command.add(String.valueOf(taskId));

        log.info("Executing Spark command: {}", String.join(" ", command));

        // 4. (关键) 异步执行命令
        // 我们不希望 API 请求被长时间阻塞
        new Thread(() -> {
            try {
                ProcessBuilder pb = new ProcessBuilder(command);
                pb.redirectErrorStream(true); // 合并标准输出和错误输出

                Process process = pb.start();

                // 实时读取 Spark 任务的日志输出
                try (var reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(process.getInputStream()))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        log.info("[Spark Job {}]: {}", taskId, line);
                    }
                }

                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    log.info("Spark Job (Task {}) finished successfully.", taskId);
                    // TODO (阶段 4): 在这里更新 MySQL 状态为 ETL_COMPLETE
                } else {
                    log.error("Spark Job (Task {}) failed with exit code {}.", taskId, exitCode);
                }

            } catch (IOException | InterruptedException e) {
                log.error("Failed to execute Spark job for Task " + taskId, e);
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}