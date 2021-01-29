import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergSQLDelete extends Setup {

    public IcebergSQLDelete() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergSQLDelete myExample = new IcebergSQLDelete();
        myExample.run();
    }

    public void run() {
        TableIdentifier table = TableIdentifier.of("default.test_table");
        // create SCD2 table
        spark.sql("CREATE TABLE " + table.toString() + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string, " +
                "    lastname string " +
                ") USING iceberg");
        // load test data
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a-V1', 'ln1';");
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a-V2', 'ln2';");
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a-V3', 'ln3';");
        spark.sql("INSERT INTO default.test_table select 1, 'customer_a-V3', 'ln4';");
        spark.sql("INSERT INTO default.test_table select 2, 'customer_b-V1', 'ln5';");

        LOGGER.warn("------- BEFORE -------------------------------");
        spark.table("default.test_table").orderBy("customer_id").show();
        LOGGER.warn("------- AFTER DELETE -------------------------------");
        spark.sql("DELETE FROM default.test_table WHERE customer_id=1 AND name='customer_a-V3'");
        spark.table("default.test_table").orderBy("customer_id").show();
        LOGGER.warn("------- AFTER DROP COLUMN -------------------------------");
        spark.sql("ALTER TABLE default.test_table DROP COLUMN name");
        spark.table("default.test_table").show();

    }

}
