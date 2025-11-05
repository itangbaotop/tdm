package top.itangbao.tdm.core.service;

import org.springframework.web.multipart.MultipartFile;

public interface StorageService {

    /**
     * 上传文件到数据湖
     *
     * @param file   上传的文件
     * @param taskId 关联的任务 ID (用于生成存储路径)
     * @return 文件的存储 URI (e.g., s3a://tdm-raw-data/task_123/raw.dat)
     */
    String upload(MultipartFile file, Long taskId);
}