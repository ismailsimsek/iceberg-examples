import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.lit;

public class IcebergHadoopTables extends Setup {

    public IcebergHadoopTables() throws Exception {
        super();
    }

    public static void main(String[] args) throws Exception {
        IcebergHadoopTables myExample = new IcebergHadoopTables();
        myExample.run();
    }

    public void run() {
        String tablePath = warehousePath + "/iceberg_v1table";
        HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());

        LOGGER.info("Creating Hadoop Table! Table Path is: {}", tablePath);
        Schema tableSchema = SparkSchemaUtil.convert(sampleDf.schema());
        PartitionSpec pspec = PartitionSpec.builderFor(tableSchema).identity("name").bucket("age", 5).build();
        tables.create(tableSchema, pspec, tablePath);
        // sampleDf.write().format("iceberg").mode("append").save(tablePath);
        spark.read().format("iceberg").load(tablePath ).show(false);

        // alter schema
        tables.load(tablePath).updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();
        Dataset<Row> newDf = sampleDf.withColumn("new_column", lit(10));
        newDf.write().format("iceberg").mode("append").save(tablePath);

        spark.read().format("iceberg").load(tablePath ).show(false);

        s3.listFiles();

        spark.read().format("iceberg").load(tablePath + "#history").show(false);
        spark.read().format("iceberg").load(tablePath + "#snapshots").show(false);
        spark.read().format("iceberg").load(tablePath + "#manifests").show(false);
        spark.read().format("iceberg").load(tablePath + "#files").show(false);
    }

}
