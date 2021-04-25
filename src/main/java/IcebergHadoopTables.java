import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class IcebergHadoopTables extends Setup {

    public IcebergHadoopTables() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergHadoopTables myExample = new IcebergHadoopTables();
        myExample.run();
    }

    public void run() {
        String tablePath = warehousePath + "/default/iceberg_v1table";
        HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());

        LOGGER.info("Creating Hadoop Table! Table Path is: {}", tablePath);
        Schema tableSchema = SparkSchemaUtil.convert(sampleDf.schema());
        PartitionSpec pspec = PartitionSpec.builderFor(tableSchema).identity("name").bucket("age", 5).build();
        Table mytable = tables.create(tableSchema, pspec, tablePath);
        // sampleDf.write().format("iceberg").mode("append").save(tablePath);
        spark.table("default.iceberg_v1table").show(false);

        // alter schema
        mytable.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();
        LOGGER.error("After Alter\n{}", tables.load(tablePath).schema());
        // avoid caching issues
        spark = spark.cloneSession();
        spark.table("default.iceberg_v1table").show(false);
        Dataset<Row> newDf = spark.sql("select 1 as age, 'test' as name, 5 as new_column ");
        newDf.write().format("iceberg").mode("append").saveAsTable("default.iceberg_v1table");
        spark.table("default.iceberg_v1table").show(false);

        s3.listFiles();

//        spark.read().format("iceberg").load(tablePath + "#history").show(false);
//        spark.read().format("iceberg").load(tablePath + "#snapshots").show(false);
//        spark.read().format("iceberg").load(tablePath + "#manifests").show(false);
//        spark.read().format("iceberg").load(tablePath + "#files").show(false);
    }

}
