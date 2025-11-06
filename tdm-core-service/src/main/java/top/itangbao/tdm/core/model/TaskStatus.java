package top.itangbao.tdm.core.model;

public enum TaskStatus {
    CREATED,         // 已创建
    UPLOADED,        // 原始文件已上传 (阶段 2)
    ETL_RUNNING,     // 清洗中 (阶段 3)
    ETL_COMPLETE,    // 清洗完成 (阶段 4)
    ANALYSIS_RUNNING, // 分析中 (阶段 5)
    ETL_FAILED,
    ANALYSIS_COMPLETE // 分析完成 (阶段 5)
}