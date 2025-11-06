### 🚀 開源 TDM 框架 - 開發任務清單 (V1)

#### 🎯 階段 1：核心服務與元數據 (Core Service & Metadata)

**目標：** 搭建項目的「大腦」和「記憶體」。建立一個可以通過 API 管理「項目」和「試驗任務」的基礎 Spring Boot 服務。

- **T-1.1 (項目初始化):** 建立 `tdm-core-service` Spring Boot 項目。

    - **依賴：** `spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `postgresql` (或 `mysql`) 驅動。

- **T-1.2 (模型定義):** 定義核心的元數據表結構（`Project`, `Task`）。

    - `Project` (項目)：`id`, `name`, `description`。

    - `Task` (試驗任務)：`id`, `project_id` (關聯), `name`, `status` (e.g., `CREATED`, `UPLOADED`, `ETL_RUNNING`, `ETL_COMPLETE`, `ANALYSIS_RUNNING`, `ANALYSIS_COMPLETE`), `raw_data_uri` (e.g., MinIO 路徑), `warehouse_table_name` (e.g., ClickHouse 表名)。

- **T-1.3 (倉儲層):** 創建 `ProjectRepository` 和 `TaskRepository` (JPA 接口)。

- **T-1.4 (API 層):** 創建 `ProjectController` 和 `TaskController`。

    - 實現 `Project` 和 `Task` 的基本 CRUD (Create, Read, Update) API。

    - **集成 OpenAPI (Swagger)：** 這是我們用來調試的「UI」。

- **T-1.5 (容器化):** 創建 `docker-compose.yml`，用於啟動 `tdm-core-service` 和 `postgresql`。


** milestone (階段 1 里程碑)：**

- 可以通過 `docker-compose up` 啟動後端服務和元數據庫。

- 可以通過 Swagger UI 成功調用 API 來「創建一個項目」和「創建一個試驗任務」。


---

#### 🎯 階段 2：數據湖集成 (Data Lake Integration)

**目標：** 實現數據 ETL 的第一步 (Extract)。將原始文件上傳並儲存到「數據湖」(MinIO)。

- **T-2.1 (抽象層):** 定義 `StorageService` 接口 (`upload`, `download`, `delete`)。

- **T-2.2 (MinIO 實現):** 創建 `MinioStorageService` 實現。

    - **配置：** 在 `application.properties` 中添加 `minio.endpoint`, `access-key`, `secret-key`。

    - **容器化：** 在 `docker-compose.yml` 中添加 `minio` 服務。

- **T-2.3 (API 更新):** 為 `TaskController` 添加一個新 API：

    - `POST /api/v1/tasks/{taskId}/upload-raw-file`

    - **核心邏輯：** 此 API 接收文件流，調用 `MinioStorageService` 將其上傳，獲取 S3 路徑 (e.g., `s3a://tdm-bucket/task-123/raw.dat`)。

    - 將此路徑更新到 `Task` 表的 `raw_data_uri` 欄位，並將 `status` 更新為 `UPLOADED`。


** milestone (階段 2 里程碑)：**

- 可以通過 API 向一個「試驗任務」上傳一個（例如 1GB 的）原始文件。

- 可以在 MinIO UI 中看到該文件，並在 PostgreSQL 中看到 `Task` 記錄的 `raw_data_uri` 已更新。


---

#### 🎯 階段 3：Spark 任務提交 (Spark Job Submission)

**目標：** 實現「管弦樂」(Orchestration) 層。讓 Spring Boot 能夠「觸發」一個 Spark 任務。

- **T-3.1 (Spark 項目):** 建立 `tdm-spark-jobs` Maven 模組。

    - 這是一個**獨立的** Spark 項目，打包成一個 Fat JAR。

    - **依賴：** `spark-core`, `spark-sql`。

- **T-3.2 (Spark 入口):** 創建一個 Spark 任務主類 `etl.CleanerJob`。

    - **邏輯 (MVP)：** 只接收一個 `--input-uri` 參數（即 `raw_data_uri`）和一個 `--task-id` 參數，然後讀取文件並打印前 10 行。

- **T-3.3 (Spark 提交服務):** 在 `tdm-core-service` 中創建 `SparkOrchestrationService`。

    - **邏輯：** 使用 `ProcessBuilder` 或更健壯的庫（如 Livy）來構建並執行一個 `spark-submit` 腳本。

- **T-3.4 (API 更新):** 創建一個 API 來觸發 ETL：

    - `POST /api/v1/tasks/{taskId}/run-etl`

    - **核心邏輯：** 讀取 `Task` 的 `raw_data_uri`，調用 `SparkOrchestrationService` 提交 `etl.CleanerJob`，並將 `status` 更新為 `ETL_RUNNING`。


** milestone (階段 3 里程碑)：**

- 可以通過 API 觸發一個（目前還是「假」的）Spark 清洗任務。

- 可以在 Spark UI 中看到任務正在運行，並在日誌中看到打印的 10 行數據。


---

#### 🎯 階段 4：可插拔倉庫 (Pluggable Warehouse)

**目標：** 實現 ETL 的最後一步 (Load)，並實現對 ClickHouse/Doris/InfluxDB 的「可插拔」寫入。

- **T-4.1 (Spark 抽象層):** 在 `tdm-spark-jobs` 中定義 `WarehouseWriter` 接口 (`trait` in Scala / `interface` in Java)。

    - **方法：** `write(DataFrame df, String tableName)`。

- **T-4.2 (Spark 實現):** 創建 `ClickHouseSparkWriter` 和 `DorisSparkWriter`。

    - **依賴：** Spark 項目需要添加 `clickhouse-jdbc` 和 `doris-spark-connector` 等依賴。

- **T-4.3 (Spark 任務升級):** 升級 `etl.CleanerJob`。

    - **新參數：** 接收 `--warehouse-profile` (e.g., "clickhouse") 和 `--warehouse-table`。

    - **核心邏輯：**

        1. 從 `input-uri` 讀取 `DataFrame`。

        2. 執行**數據清洗** (e.g., `df.filter(...)`)。

        3. 使用工廠模式，根據 `--warehouse-profile` 參數實例化正確的 `WarehouseWriter`。

        4. 調用 `writer.write(cleanedDf, "task_123_clean_data")`。

- **T-4.4 (Spring Boot 升級):** 升級 `run-etl` API (T-3.4) 的邏輯，使其能夠傳遞這些新參數。


** milestone (階段 4 里程碑)：**

- 可以通過 API 觸發一個**完整**的 ETL 流程。

- 原始文件 (MinIO) -> Spark 清洗 -> 寫入 ClickHouse (或 Doris)。

- 我們可以用 ClickHouse 客戶端查詢到清洗後的時序數據。


---

#### 🎯 階段 5：閉環分析 (Closed-Loop Analysis)

**目標：** 實現第二個核心工作流：從 Web 端觸發 Spark 執行複雜分析 (FFT)。

- **T-5.1 (Spark 任務):** 在 `tdm-spark-jobs` 中創建 `analysis.FFTJob`。

    - **參數：** `--input-profile` (e.g., "clickhouse"), `--input-query`, `--result-id`。

    - **核心邏輯：**

        1. 使用 `WarehouseReader`（類似 T-4.1）從 ClickHouse 讀取數據。

        2. 執行 `spark.ml.feature.FourierExpander` (FFT)。

        3. 將結果（通常較小）**寫回 PostgreSQL 元數據庫**的 `analysis_results` 表中（關聯 `result-id`）。

- **T-5.2 (元數據庫):** 在 PostgreSQL 中創建 `analysis_results` 表。

- **T-5.3 (API 更新):** 創建分析 API：

    - `POST /api/v1/tasks/{taskId}/run-analysis`（觸發 `FFTJob`，返回 `resultId`）。

    - `GET /api/v1/analysis-results/{resultId}`（輪詢此 API 以獲取分析結果）。


** milestone (階段 5 里程碑)：**

- 可以通過 API 觸發對 ClickHouse 中數據的 FFT 分析。

- 可以在 Spark 任務完成後，通過 `GET` API 獲取到 JSON 格式的頻譜結果。

- **項目 MVP 核心功能閉環！**