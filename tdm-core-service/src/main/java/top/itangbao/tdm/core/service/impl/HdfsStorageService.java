package top.itangbao.tdm.core.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import top.itangbao.tdm.core.service.StorageService;

import java.io.IOException;
import java.io.InputStream;

@Service
@Slf4j
@ConditionalOnProperty(name = "storage.type", havingValue = "hdfs")
public class HdfsStorageService implements StorageService {

    @Value("${storage.hdfs.namenode}")
    private String namenode;

    @Value("${storage.hdfs.base-path:/tdm}")
    private String basePath;

    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", namenode);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        
        // Docker HDFS配置
        conf.set("hadoop.security.authentication", "simple");
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.client.use.datanode.hostname", "true");
        
        // 设置用户
        System.setProperty("HADOOP_USER_NAME", "root");
        
        return FileSystem.get(conf);
    }

    @Override
    public String upload(MultipartFile file, String bucketName, String objectName) {
        try (FileSystem fs = getFileSystem();
             InputStream inputStream = file.getInputStream()) {
            
            String hdfsPath = String.format("%s/%s/%s", basePath, bucketName, objectName);
            Path path = new Path(hdfsPath);
            
            // 创建目录
            fs.mkdirs(path.getParent());
            
            // 上传文件
            org.apache.hadoop.fs.FSDataOutputStream outputStream = fs.create(path, true);
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.close();
            
            log.info("File uploaded to HDFS: {}", hdfsPath);
            return String.format("hdfs://%s%s", namenode.replace("hdfs://", ""), hdfsPath);
            
        } catch (IOException e) {
            log.error("Failed to upload file to HDFS", e);
            throw new RuntimeException("HDFS upload failed", e);
        }
    }

    @Override
    public InputStream download(String uri) {
        try {
            FileSystem fs = getFileSystem();
            Path path = new Path(uri);
            return fs.open(path);
        } catch (IOException e) {
            log.error("Failed to download file from HDFS: {}", uri, e);
            throw new RuntimeException("HDFS download failed", e);
        }
    }

    @Override
    public void delete(String uri) {
        try (FileSystem fs = getFileSystem()) {
            Path path = new Path(uri);
            fs.delete(path, false);
            log.info("File deleted from HDFS: {}", uri);
        } catch (IOException e) {
            log.error("Failed to delete file from HDFS: {}", uri, e);
            throw new RuntimeException("HDFS delete failed", e);
        }
    }
}