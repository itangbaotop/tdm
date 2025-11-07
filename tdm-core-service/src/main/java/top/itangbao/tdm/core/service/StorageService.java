package top.itangbao.tdm.core.service;

import org.springframework.web.multipart.MultipartFile;
import java.io.InputStream;

public interface StorageService {


    /**
     * 通用上传方法
     */
    String upload(MultipartFile file, String bucketName, String objectName);

    /**
     * 下载文件
     */
    InputStream download(String uri);

    /**
     * 删除文件
     */
    void delete(String uri);
}