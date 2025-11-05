package top.itangbao.tdm.core.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import top.itangbao.tdm.core.model.Project;
import top.itangbao.tdm.core.model.Task;
import top.itangbao.tdm.core.model.TaskStatus;
import top.itangbao.tdm.core.repository.ProjectRepository;
import top.itangbao.tdm.core.repository.TaskRepository;
import top.itangbao.tdm.core.service.SparkOrchestrationService;
import top.itangbao.tdm.core.service.StorageService;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskRepository taskRepository;
    private final ProjectRepository projectRepository;
    private final StorageService storageService;
    private final SparkOrchestrationService sparkService;

    // (DTO) 用于创建任务的请求体
    record CreateTaskRequest(String name, Long projectId) {}

    // API: 创建一个新任务 (阶段 1)
    @PostMapping
    public ResponseEntity<Task> createTask(@RequestBody CreateTaskRequest request) {
        Project project = projectRepository.findById(request.projectId)
                .orElseThrow(() -> new RuntimeException("Project not found: " + request.projectId));

        Task task = new Task();
        task.setName(request.name);
        task.setProject(project);
        task.setStatus(TaskStatus.CREATED); // 初始状态

        Task savedTask = taskRepository.save(task);
        return ResponseEntity.ok(savedTask);
    }

    // API: 获取一个任务的详情 (阶段 1)
    @GetMapping("/{taskId}")
    public ResponseEntity<Task> getTaskById(@PathVariable Long taskId) {
        return taskRepository.findById(taskId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // API: 【阶段 2 核心】上传原始文件
    @PostMapping("/{taskId}/upload-raw-file")
    public ResponseEntity<Map<String, String>> uploadRawFile(
            @PathVariable Long taskId,
            @RequestParam("file") MultipartFile file) {

        // 1. 验证任务是否存在
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found: " + taskId));

        // 2. (核心) 调用服务上传文件
        String rawDataUri = storageService.upload(file, taskId);

        // 3. (核心) 更新元数据
        task.setRawDataUri(rawDataUri);
        task.setStatus(TaskStatus.UPLOADED); // 更新状态
        taskRepository.save(task);

        return ResponseEntity.ok(Map.of("message", "File uploaded successfully", "rawDataUri", rawDataUri));
    }

    @PostMapping("/{taskId}/run-etl")
    public ResponseEntity<Map<String, String>> runEtlJob(@PathVariable Long taskId) {

        // 1. 验证任务是否存在且文件已上传
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found: " + taskId));

        if (task.getRawDataUri() == null || task.getRawDataUri().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Raw data file has not been uploaded yet."));
        }

        if (task.getStatus() == TaskStatus.ETL_RUNNING) {
            return ResponseEntity.status(409).body(Map.of("message", "ETL job is already running for this task.")); // 409 Conflict
        }

        // 2. (核心) 提交 Spark 任务
        sparkService.submitEtlJob(task.getId(), task.getRawDataUri());

        // 3. 更新元数据状态
        task.setStatus(TaskStatus.ETL_RUNNING);
        taskRepository.save(task);

        return ResponseEntity.ok(Map.of(
                "message", "ETL job submitted successfully.",
                "taskId", String.valueOf(taskId)
        ));
    }
}