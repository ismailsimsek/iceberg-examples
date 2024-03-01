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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Setup {

    protected static final Logger LOGGER = LoggerFactory.getLogger(Setup.class);
    SparkConf sparkconf = new SparkConf();
    Minio s3 = new Minio();
    String warehousePath = "s3a://" + Minio.TEST_BUCKET + "/my-iceberg-warehouse";
    SparkSession spark;
    Map<String, String> icebergOptions = new ConcurrentHashMap<>();
    Dataset<Row> sampleDf;

    public Setup() throws Exception {
        s3.start();
        sparkconf.setAppName("CDC-S3-Batch-Spark-Sink")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.eventLog.enabled", "false")
                .set("spark.hadoop.fs.s3a.access.key", Minio.MINIO_ACCESS_KEY)
                .set("spark.hadoop.fs.s3a.secret.key", Minio.MINIO_SECRET_KEY)
                // minio specific setting using minio as S3
                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + s3.getMappedPort())
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                // enable iceberg SQL Extensions
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                //.set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                .set("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
                .set("spark.sql.warehouse.dir", warehousePath)
        ;
        icebergOptions.put("warehouse", warehousePath);
        icebergOptions.put("type", "hadoop");
        icebergOptions.put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");

        spark = SparkSession
                .builder()
                .config(this.sparkconf)
                .getOrCreate();

        LOGGER.warn(spark.sparkContext().getConf().toDebugString());

        List<String> jsonData = Arrays
                .asList("{'name':'User-1', 'age':1122}\n{'name':'User-2', 'age':1130}\n{'name':'User-3', 'age':1119}"
                        .split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = this.spark.createDataset(jsonData, Encoders.STRING());
        sampleDf = spark.read().json(ds).toDF();

        LOGGER.warn("Spark Version:{}", spark.version());
    }

}
