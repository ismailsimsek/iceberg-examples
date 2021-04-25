import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;

public class IcebergSCD2 extends Setup {

    public IcebergSCD2() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergSCD2 myExample = new IcebergSCD2();
        myExample.run();
    }

    public void run() throws TableAlreadyExistsException {
        TableIdentifier table = TableIdentifier.of("default.scd2_table");
        // create SCD2 table
        spark.sql("CREATE TABLE " + table.toString() + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string ," +
                "    current boolean," +
                "    effective_date date," +
                "    end_date date" +
                ") USING iceberg");
        // load test data
        spark.sql("INSERT INTO default.scd2_table " +
                "select 1, 'customer_a-V1', false, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('2020-01-12', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.scd2_table " +
                "select 1, 'customer_a-V2', true, to_date('2020-01-12', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.scd2_table " +
                "select 2, 'customer_b-V1', true, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");

        spark.sql("CREATE TABLE default.stg_scd2_table (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string ," +
                "    effective_date date" +
                ") USING iceberg");

        spark.sql("INSERT INTO default.stg_scd2_table " +
                "select 1, 'customer_a-V3', to_date('2020-02-15', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.stg_scd2_table " +
                "select 2, 'customer_b-V2', to_date('2020-02-15', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.stg_scd2_table " +
                "select 3, 'customer_c-V1', to_date('2020-02-15', 'yyyy-MM-dd');");

        LOGGER.warn("------- BEFORE -------------------------------");
        spark.table("default.scd2_table").orderBy("customer_id", "effective_date").show();
        LOGGER.warn("------- NEW DATA -------------------------------");
        spark.table("default.stg_scd2_table").show();

        String merge = "MERGE INTO default.scd2_table t \n" +
                "USING ( " +
                // new data goes to insert
                "    SELECT customer_id, name, effective_date, to_date('9999-12-31', 'yyyy-MM-dd') as end_date FROM default.stg_scd2_table " +
                "    UNION ALL" +
                // update exiting records and close them
                "    SELECT t.customer_id, t.name, t.effective_date, s.effective_date as end_date FROM default.stg_scd2_table s" +
                "    INNER JOIN default.scd2_table t on s.customer_id = t.customer_id AND t.current = true " +
                ") s \n" +
                "ON s.customer_id = t.customer_id AND s.effective_date = t.effective_date \n" +
                // close last record.
                "WHEN MATCHED \n" +
                "  THEN UPDATE SET t.current = false, t.end_date = s.end_date \n" +
                "WHEN NOT MATCHED THEN \n" +
                "   INSERT(customer_id, name, current, effective_date, end_date) \n" +
                "   VALUES(s.customer_id, s.name, true, s.effective_date, s.end_date)" +
                ";";
        spark.sql(merge);

        LOGGER.warn("------- AFTER -------------------------------");
        spark.table("default.scd2_table").orderBy("customer_id", "effective_date").show();
        LOGGER.warn("------- FINAL S3 FILES -------------------------------");
        s3.listFiles();
    }

}
