package top.itangbao.tdm.core.service;

public interface SparkOrchestrationService {
    /**
     * 异步提交一个 ETL Spark 任务
     * @param taskId 任务 ID
     * @param inputUri 要处理的原始文件的 URI (e.g., s3a://...)
     */
    void submitEtlJob(Long taskId, String inputUri);
}