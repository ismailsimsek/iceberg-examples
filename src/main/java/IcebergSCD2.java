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

        spark.sql("CREATE TABLE default.stg_scd2_table (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string ," +
                "    effective_date date" +
                ") USING iceberg");

        spark.sql("INSERT INTO default.stg_scd2_table " +
                "select 1, 'customer_a', to_date('2020-01-01', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.stg_scd2_table " +
                "select 2, 'customer_b', to_date('2020-01-01', 'yyyy-MM-dd');");

        String merge = "\n" +
                "                MERGE INTO spark_catalog.default.scd2_table t\n" +
                "                USING (\n" +
                "                SELECT customer_id, name, effective_date from spark_catalog.default.stg_scd2_table) s\n" +
                "                ON s.customer_id = t.customer_id\n" +
                "                WHEN MATCHED \n" +
                "                THEN UPDATE SET t.current = false, t.effective_date = s.effective_date\n" +
                "                WHEN NOT MATCHED THEN\n" +
                "                INSERT(customer_id, name, current, effective_date, end_date)\n" +
                "                VALUES(s.customer_id, s.name, true, s.effective_date, null)" +
                ";";

        spark.sql(merge);

        spark.table("default.scd2_table").show();
        //spark.sql("select * from default.test_table").show();

    }

}
