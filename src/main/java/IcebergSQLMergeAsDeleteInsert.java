import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergSQLMergeAsDeleteInsert extends Setup {

    public IcebergSQLMergeAsDeleteInsert() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergSQLMergeAsDeleteInsert myExample = new IcebergSQLMergeAsDeleteInsert();
        myExample.run();
    }

    public void run() {
        TableIdentifier table = TableIdentifier.of("default.test_table");
        // create SCD2 table
        spark.sql("CREATE TABLE " + table + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string, " +
                "    record_change_timestamp integer " +
                ") USING iceberg");
        // load test data
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a', 1;");
        spark.sql("INSERT INTO default.test_table select 2, 'customer_b', 1;");
        spark.sql("INSERT INTO default.test_table select 3, 'customer_b', 2;");

        LOGGER.warn("------- BEFORE -------------------------------");
        spark.table("default.test_table").orderBy("customer_id").show(false);

        String merge = "MERGE INTO default.test_table t \n" +
                "USING ( \n" +
                // new data goes to insert
                "    SELECT 1 as customer_id, 'customer_c-insert(with merge sql)' as name, 1 as record_change_timestamp \n" +
                "    UNION ALL \n" +
                // update exiting record
                "    SELECT 2 as customer_id, 'customer_b-updated(with merge sql)' as name, 2 as record_change_timestamp \n" +
                "    UNION ALL \n" +
                // update exiting record
                "    SELECT 3 as customer_id, 'customer_b-updated(with merge sql)' as name, 2 as record_change_timestamp \n" +
                ") s \n" +
                "ON s.customer_id = t.customer_id \n" +
                // close last record.
                "WHEN MATCHED and t.record_change_timestamp < s.record_change_timestamp \n" +
                "  THEN UPDATE SET t.name = s.name, t.record_change_timestamp = s.record_change_timestamp \n" +
                "WHEN NOT MATCHED THEN \n" +
                "   INSERT(customer_id, name, record_change_timestamp) \n" +
                "   VALUES(s.customer_id, s.name, s.record_change_timestamp)" +
                ";";
        LOGGER.info("merge Query\n{}", merge);
        spark.sql(merge);
        LOGGER.warn("------- AFTER MERGE -------------------------------");
        spark.cloneSession().table("default.test_table").show(false);
    }

}
