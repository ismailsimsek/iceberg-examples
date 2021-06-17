import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergSQLMerge extends Setup {

    public IcebergSQLMerge() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergSQLMerge myExample = new IcebergSQLMerge();
        myExample.run();
    }

    public void run() {
        TableIdentifier table = TableIdentifier.of("default.test_table");
        // create SCD2 table
        spark.sql("CREATE TABLE " + table + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string, " +
                "    lastname string " +
                ") USING iceberg");
        // load test data
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a', 'lastname-1';");
        spark.sql("INSERT INTO default.test_table select 2, 'customer_b', 'lastname-2';");

        LOGGER.warn("------- BEFORE -------------------------------");
        spark.table("default.test_table").orderBy("customer_id").show(false);

        String merge = "MERGE INTO default.test_table t \n" +
                "USING ( \n" +
                // new data goes to insert
                "    SELECT 3 as customer_id, 'customer_c-insert(with merge sql)' as name, 'lastname-3-insert(with merge sql)' as lastname \n" +
                "    UNION ALL \n" +
                // update exiting record
                "    SELECT 2 as customer_id, 'customer_b-updated(with merge sql)' as name, 'lastname-2-updated(with merge sql)' as lastname \n" +
                ") s \n" +
                "ON s.customer_id = t.customer_id \n" +
                // close last record.
                "WHEN MATCHED \n" +
                "  THEN UPDATE SET t.name = s.name, t.lastname = s.lastname \n" +
                "WHEN NOT MATCHED THEN \n" +
                "   INSERT(customer_id, name, lastname) \n" +
                "   VALUES(s.customer_id, s.name, s.lastname)" +
                ";";
        LOGGER.info("merge Query\n{}", merge);
        spark.sql(merge);
        LOGGER.warn("------- AFTER MERGE -------------------------------");
        spark.cloneSession().table("default.test_table").show(false);

        String mergev2 = "MERGE INTO default.test_table t \n" +
                "USING ( \n" +
                // new data goes to insert
                "    SELECT 4 as customer_id, 'customer_c-insert(with merge *)' as name, 'lastname-3-insert(with merge *)' as lastname, 'a@b.com' as email \n" +
                "    UNION ALL \n" +
                // update exiting record
                "    SELECT 5 as customer_id, 'customer_b-updated(with merge *)' as name, 'lastname-2-updated(with merge *)' as lastname, 'b@c.com' as email \n" +
                ") s \n" +
                "ON s.customer_id = t.customer_id \n" +
                // close last record.
                "WHEN MATCHED \n" +
                "  THEN UPDATE SET * \n" +
                "WHEN NOT MATCHED THEN \n" +
                "   INSERT *" +
                ";";
        LOGGER.info("merge Query\n{}", mergev2);
        spark.sql(mergev2);
        LOGGER.warn("------- AFTER MERGE WITH * extra column -------------------------------");
        spark.cloneSession().table("default.test_table").show(false);
        spark.sql("ALTER TABLE default.test_table ADD COLUMN email string");
        spark.cloneSession().table("default.test_table").show(false);
        spark.sql(mergev2);
        spark.cloneSession().table("default.test_table").show(false);

        String mergev3 = "MERGE INTO default.test_table t \n" +
                "USING ( \n" +
                // new data goes to insert
                "    SELECT 6 as customer_id, 'customer_c-insert(with merge *)' as name, 'a@b.com' as email \n" +
                "    UNION ALL \n" +
                // update exiting record
                "    SELECT 7 as customer_id, 'customer_b-updated(with merge *)' as name, 'b@c.com' as email \n" +
                ") s \n" +
                "ON s.customer_id = t.customer_id \n" +
                // close last record.
                "WHEN MATCHED \n" +
                "  THEN UPDATE SET * \n" +
                "WHEN NOT MATCHED THEN \n" +
                "   INSERT *" +
                ";";
        LOGGER.info("merge Query\n{}", mergev3);
        spark.sql(mergev3);
        LOGGER.warn("------- AFTER MERGE WITH * less column -------------------------------");
        spark.cloneSession().table("default.test_table").show(false);

    }

}
