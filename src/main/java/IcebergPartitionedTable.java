import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.hadoop.HadoopTables;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class IcebergPartitionedTable extends Setup {

    public IcebergPartitionedTable() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergPartitionedTable myExample = new IcebergPartitionedTable();
        myExample.run();
    }

    public void run() {
        TableIdentifier table = TableIdentifier.of("default.partitioned_table");
        // create table 
        spark.sql("CREATE TABLE " + table + " (" +
                "    customer_id bigint COMMENT 'unique id'," +
                "    name string ," +
                "    current boolean," +
                "    effective_date date," +
                "    end_date date" +
                ") USING iceberg " +
                " " +
                "");

        // load test data
        spark.sql("INSERT INTO default.partitioned_table " +
                "select 1, 'customer_a-V1', false, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('2020-01-12', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.partitioned_table " +
                "select 1, 'customer_a-V2', true, to_date('2020-01-12', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");
        spark.sql("INSERT INTO default.partitioned_table " +
                "select 2, 'customer_b-V1', true, to_date('2020-01-01', 'yyyy-MM-dd'), to_date('9999-12-31', 'yyyy-MM-dd');");

        LOGGER.warn("------- TABLE -------------------------------");
        HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());
        String tablePath = warehousePath + "/default/partitioned_table";
        Table ptttable = tables.load(tablePath);
        LOGGER.info("schema:{}", ptttable.schema());
        LOGGER.info("spec:{}", ptttable.spec());

        GenericRecord record = GenericRecord.create(ptttable.schema().asStruct());
        record.setField("customer_id", 1);
        record.setField("name", "customer_c-V1");
        record.setField("effective_date", LocalDate.parse("2020-03-01", DateTimeFormatter.ISO_LOCAL_DATE));
        record.setField("current", false);

        PartitionKey partitionKey = new PartitionKey(ptttable.spec(), ptttable.schema());
        InternalRecordWrapper wrapper = new InternalRecordWrapper(record.struct());
        partitionKey.partition(wrapper.wrap(record));

        LOGGER.info("record:{}", record);
        LOGGER.info("PartitionKey:{}", partitionKey);
        LOGGER.info("newDataLocation:{}", ptttable.locationProvider().newDataLocation(partitionKey.toPath()));
        LOGGER.info("newDataLocation:{}", ptttable.locationProvider().newDataLocation(""));
        LOGGER.info("newDataLocation:{}", ptttable.locationProvider().newDataLocation("test"));
        assert !ptttable.spec().partitionToPath(record).equals("effective_date_month=2020-03/name_trunc=customer_c") : "wrong PTT";

        LOGGER.warn("------- AFTER -------------------------------");
        spark.table("default.partitioned_table").orderBy("customer_id", "effective_date").show();
        LOGGER.warn("------- FINAL S3 FILES -------------------------------");
        s3.listFiles();
    }

}