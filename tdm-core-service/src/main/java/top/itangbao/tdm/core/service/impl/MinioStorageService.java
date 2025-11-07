package top.itangbao.tdm.core.service.impl;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import top.itangbao.tdm.core.service.StorageService;

import java.io.InputStream;

import jakarta.annotation.PostConstruct;

@Service
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(name = "storage.type", havingValue = "minio", matchIfMissing = true)
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
    public String upload(MultipartFile file, String bucketName, String objectName) {
        try {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(file.getInputStream(), file.getSize(), -1)
                            .contentType(file.getContentType())
                            .build()
            );

            String uri = String.format("s3a://%s/%s", bucketName, objectName);
            log.info("File uploaded to MinIO: {}", uri);
            return uri;
        } catch (Exception e) {
            throw new RuntimeException("MinIO upload failed", e);
        }
    }

    @Override
    public InputStream download(String uri) {
        // 简化实现，实际需要解析URI
        throw new UnsupportedOperationException("MinIO download not implemented");
    }

    @Override
    public void delete(String uri) {
        // 简化实现，实际需要解析URI
        throw new UnsupportedOperationException("MinIO delete not implemented");
    }
}