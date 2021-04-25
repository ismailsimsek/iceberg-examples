import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class IcebergJavaApiAppend extends Setup {


    ArrayList<Record> sampleIcebergrecords = Lists.newArrayList();
    ArrayList<Record> sampleIcebergrecords2 = Lists.newArrayList();

    public IcebergJavaApiAppend() throws Exception {
        super();

        GenericRecord record = GenericRecord.create(SparkSchemaUtil.convert(sampleDf.schema()));
        sampleIcebergrecords.add(record.copy("age", 29L, "name", "GenericRecord-a"));
        sampleIcebergrecords.add(record.copy("age", 43L, "name", "GenericRecord-b"));

        sampleIcebergrecords2.add(record.copy("age", 129L, "name", "GenericRecord-2-c"));
        sampleIcebergrecords2.add(record.copy("age", 123L, "name", "GenericRecord-2-d"));

    }

    public static void main(String[] args) throws Exception {
        IcebergJavaApiAppend myExample = new IcebergJavaApiAppend();
        myExample.run();
    }

    public void run() throws IOException, NoSuchTableException, TableAlreadyExistsException, NoSuchNamespaceException {
        // get catalog from spark
        SparkSessionCatalog sparkSessionCatalog = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
        Identifier tableIdentifier = Identifier.of(Namespace.of("default").levels(), "iceberg_table");
        Schema tableSchema = SparkSchemaUtil.convert(sampleDf.schema());
        LOGGER.info("Iceberg Table schema is: {}", tableSchema.asStruct());

        Map<String, String> options = Maps.newHashMap();
        Transform[] transforms = {};
        sparkSessionCatalog.createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
        SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);

        LOGGER.warn("------------AFTER Spark SQL INSERT----------------");
        spark.sql("INSERT INTO default.iceberg_table VALUES (10,'spark sql-insert')");
        spark.sql("select * from default.iceberg_table").show();
        LOGGER.warn("------------AFTER Dataframe writeTo----------------");
        sampleDf.writeTo("default.iceberg_table").append();
        spark.sql("select * from default.iceberg_table").show();

        //---------- append data to table
        FileIO outFile = sparkTable.table().io();
        OutputFile out = outFile.newOutputFile(sparkTable.table().locationProvider().newDataLocation(UUID.randomUUID() + "-001"));
        FileAppender<Record> writer = Parquet.write(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(sparkTable.table())
                .overwrite()
                .build();
        try (Closeable toClose = writer) {
            writer.addAll(sampleIcebergrecords);
        }

        DataFile dataFile = DataFiles.builder(sparkTable.table().spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();

        sparkTable.table().newAppend()
                .appendFile(dataFile)
                .commit();
        LOGGER.warn("------------AFTER API APPEND----------------");
        spark.sql("select * from default.iceberg_table").show();
        LOGGER.warn("------------FINAL S3 FILE LIST ----------------");
        s3.listFiles();

    }

}
