package top.itangbao.tdm.spark.warehouse;

public class WarehouseWriterFactory {
    
    public static WarehouseWriter createWriter(String profile) {
        switch (profile.toLowerCase()) {
            case "clickhouse":
                return new ClickHouseSparkWriter();
            case "doris":
                // TODO: 实现DorisSparkWriter
                throw new UnsupportedOperationException("Doris writer not implemented yet");
            default:
                throw new IllegalArgumentException("Unknown warehouse profile: " + profile);
        }
    }
}