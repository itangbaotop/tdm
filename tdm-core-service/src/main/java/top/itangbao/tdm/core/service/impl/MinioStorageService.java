package top.itangbao.tdm.core.service.impl;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import top.itangbao.tdm.core.service.StorageService;

import jakarta.annotation.PostConstruct;

@Service
@Slf4j
@RequiredArgsConstructor
public class MinioStorageService implements StorageService {

    private final MinioClient minioClient;

    @Value("${minio.bucket}")
    private String bucketName;

    // 在服务启动时检查存储桶是否存在，如果不存在则创建
    @PostConstruct
    private void initBucket() {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("MinIO bucket '{}' created.", bucketName);
            } else {
                log.info("MinIO bucket '{}' already exists.", bucketName);
            }
        } catch (Exception e) {
            log.error("Error while checking or creating MinIO bucket", e);
            throw new RuntimeException("Could not initialize MinIO bucket", e);
        }
    }

    @Override
    public String upload(MultipartFile file, Long taskId) {
        try {
            // 1. 生成对象名称 (路径)
            // e.g., task_123/raw_data.dat
            String originalFilename = file.getOriginalFilename() != null ? file.getOriginalFilename() : "raw.dat";
            String objectName = String.format("task_%d/%s", taskId, originalFilename);

            // 2. 执行上传
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(file.getInputStream(), file.getSize(), -1) // 使用流式上传
                            .contentType(file.getContentType())
                            .build()
            );

            // 3. 返回一个标准化的 URI (s3a:// 是 Spark/Hadoop 识别的协议)
            // 这就是我们将存储在 MySQL 中的 `raw_data_uri`
            String uri = String.format("s3a://%s/%s", bucketName, objectName);
            log.info("File uploaded successfully. URI: {}", uri);
            return uri;

        } catch (Exception e) {
            log.error("Error uploading file to MinIO", e);
            throw new RuntimeException("Error uploading file", e);
        }
    }
}