package top.itangbao.tdm.core.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import top.itangbao.tdm.core.model.Task;
import top.itangbao.tdm.core.model.TaskStatus;
import top.itangbao.tdm.core.repository.TaskRepository;
import top.itangbao.tdm.core.service.SparkOrchestrationService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SparkOrchestrationServiceImpl implements SparkOrchestrationService {

    @Value("${spark.submit.path}")
    private String sparkSubmitPath;

    @Value("${spark.master.url}")
    private String sparkMasterUrl;

    @Value("${spark.job.jar-path}")
    private String sparkJobJarPath;

    @Value("${warehouse.clickhouse.host}")
    private String warehouseHost;
    @Value("${warehouse.clickhouse.port.http}")
    private String warehousePort;
    @Value("${warehouse.clickhouse.database}")
    private String warehouseDatabase;


    private final TaskRepository taskRepository;

    @Override
    public void submitEtlJob(Long taskId, String inputUri) {
        log.info("Submitting Spark ETL job for Task ID: {}", taskId);

        String jobJarAbsolutePath;
        try {
            jobJarAbsolutePath = new File(sparkJobJarPath).getCanonicalPath();
        } catch (IOException e) {
            log.error("Could not resolve spark job jar path: {}", sparkJobJarPath, e);
            throw new RuntimeException("Spark job jar not found", e);
        }
        log.info("Spark Job JAR path: {}", jobJarAbsolutePath);

        // 使用动态表名（基于CSV文件名或任务ID）
        String warehouseTable = String.format("%s.task_%d_data", warehouseDatabase, taskId);


        // 2. 构建 spark-submit 命令
        List<String> command = getCommand(taskId, inputUri, jobJarAbsolutePath, warehouseTable);

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

                updateTaskStatus(taskId, exitCode);

            } catch (IOException | InterruptedException e) {
                log.error("Failed to execute Spark job for Task {}", taskId, e);
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    @NotNull
    private List<String> getCommand(Long taskId, String inputUri, String jobJarAbsolutePath, String warehouseTable) {
        List<String> command = new ArrayList<>();
        command.add(sparkSubmitPath);

        command.add("--class");
        command.add("top.itangbao.tdm.spark.etl.CleanerJob");

        command.add("--master");
        command.add(sparkMasterUrl);

        command.add("--conf");
        command.add("spark.hadoop.fs.s3a.endpoint=http://localhost:9000");

        command.add(jobJarAbsolutePath); // JAR 包

        // 3. 添加 Spark 任务所需的参数
        command.add("--input-uri");
        command.add(inputUri);
        command.add("--task-id");
        command.add(String.valueOf(taskId));

        command.add("--warehouse-host");
        command.add(warehouseHost);
        command.add("--warehouse-port");
        command.add(warehousePort);
        command.add("--warehouse-table");
        command.add(warehouseTable);

        log.info("Executing Spark command (in WSL): {}", String.join(" ", command));
        return command;
    }

    private void updateTaskStatus(Long taskId, int exitCode) {
        try {
            Task task = taskRepository.findById(taskId)
                    .orElseThrow(() -> new RuntimeException("Task not found while updating status: " + taskId));

            if (exitCode == 0) {
                log.info("Spark Job (Task {}) finished successfully.", taskId);
                task.setStatus(TaskStatus.ETL_COMPLETE); // 成功

            } else {
                log.error("Spark Job (Task {}) failed with exit code {}.", taskId, exitCode);
                task.setStatus(TaskStatus.ETL_FAILED); // 失败
            }
            taskRepository.save(task);

        } catch (Exception e) {
            log.error("CRITICAL: Failed to update task status for Task {} after job completion.", taskId, e);
        }
    }
}