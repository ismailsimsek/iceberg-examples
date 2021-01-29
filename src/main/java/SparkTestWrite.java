import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public abstract class SparkTestWrite {

    protected static final Logger LOGGER = LoggerFactory.getLogger(SparkTestWrite.class);

    public static void main(String[] args) throws Exception {

        SparkConf sparkconf = new SparkConf();
        Minio s3 = new Minio();
        String warehousePath = "fs-warehouse";
        SparkSession spark;
        Dataset<Row> sampleDf;

        s3.start();
        sparkconf.setAppName("CDC-S3-Batch-Spark-Sink")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.eventLog.enabled", "false")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                .set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                .set("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
                .set("spark.sql.warehouse.dir", warehousePath)
        ;

        spark = SparkSession
                .builder()
                .config(sparkconf)
                .getOrCreate();

        LOGGER.warn(spark.sparkContext().getConf().toDebugString());

        List<String> jsonData = Arrays
                .asList("{'name':'User-1', 'age':1122}\n{'name':'User-2', 'age':1130}\n{'name':'User-3', 'age':1119}"
                        .split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        sampleDf = spark.read().json(ds).toDF();

        sampleDf.write().format("iceberg").mode("append").save(warehousePath + "/test_table");

        LOGGER.warn("Spark Version:{}", spark.version());
    }

}
