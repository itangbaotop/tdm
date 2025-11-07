package top.itangbao.tdm.core.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import top.itangbao.tdm.core.service.StorageService;
import top.itangbao.tdm.core.service.impl.HdfsStorageService;
import top.itangbao.tdm.core.service.impl.MinioStorageService;

import java.util.Optional;

@Configuration
public class StorageConfig {

    @Value("${storage.type:minio}")
    private String storageType;

    @Bean
    public StorageService storageService(@Autowired(required = false) MinioStorageService minioStorageService, 
                                       @Autowired(required = false) HdfsStorageService hdfsStorageService) {
        switch (storageType.toLowerCase()) {
            case "hdfs":
                if (hdfsStorageService == null) {
                    throw new IllegalStateException("HDFS storage service not available");
                }
                return hdfsStorageService;
            case "minio":
            default:
                if (minioStorageService == null) {
                    throw new IllegalStateException("MinIO storage service not available");
                }
                return minioStorageService;
        }
    }
}