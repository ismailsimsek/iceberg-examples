import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
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
import java.util.concurrent.ConcurrentHashMap;

public class IcebergApiAppend extends Setup {


    ArrayList<Record> sampleIcebergrecords = Lists.newArrayList();
    ArrayList<Record> sampleIcebergrecords2 = Lists.newArrayList();

    public IcebergApiAppend() throws Exception {
        super();

        GenericRecord record = GenericRecord.create(SparkSchemaUtil.convert(sampleDf.schema()));
        sampleIcebergrecords.add(record.copy("age", 29L, "name", "GenericRecord-a"));
        sampleIcebergrecords.add(record.copy("age", 43L, "name", "GenericRecord-b"));

        sampleIcebergrecords2.add(record.copy("age", 129L, "name", "GenericRecord-2-c"));
        sampleIcebergrecords2.add(record.copy("age", 123L, "name", "GenericRecord-2-d"));

    }

    public static void main(String[] args) throws Exception {
        IcebergApiAppend myExample = new IcebergApiAppend();
        myExample.run();
        // run append
        // run upsert
    }

    public void run() throws IOException, NoSuchTableException, TableAlreadyExistsException, NoSuchNamespaceException {
        // get catalog from spark
        SparkSessionCatalog sparkSessionCatalog = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
        Identifier tableIdentifier = Identifier.of(Namespace.of("default").levels(), "iceberg_table");
        Schema tableSchema = SparkSchemaUtil.convert(sampleDf.schema());
        // PartitionSpec tablePartitinSpec = PartitionSpec.builderFor(tableSchema).bucket("age", 5).build();
        LOGGER.info("Iceberg Table schema is: {}", tableSchema.asStruct());

        Map<String, String> options = Maps.newHashMap();
        Transform[] transforms = {};
        sparkSessionCatalog.createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
        SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
        LOGGER.debug("Spark table location: '{}'", sparkTable.table().location());

        spark.sql("INSERT INTO default.iceberg_table VALUES (10,'spark sql-insert')");
        sampleDf.writeTo("default.iceberg_table").append();
        spark.sql("select * from default.iceberg_table").show();

        s3.listFiles();

        // append data to table
        FileIO outFile = sparkTable.table().io();
        OutputFile out = outFile.newOutputFile(sparkTable.table().locationProvider().newDataLocation(UUID.randomUUID()+"-001"));
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

        LOGGER.debug("Table history size pre commit! {}", sparkTable.table().history().size());
        sparkTable.table().newAppend()
                .appendFile(dataFile)
                .commit();
        LOGGER.debug("Table history size post commit! {}", sparkTable.table().history().size());
        spark.sql("select * from default.iceberg_table").show();
        s3.listFiles();

        OutputFile out2 = outFile.newOutputFile(sparkTable.table().locationProvider().newDataLocation("data_append_using_iceberg.parquet"));
        FileAppender<Record> writer2 = Parquet.write(out2)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(sparkTable.table())
                .overwrite()
                .build();
        try (Closeable toClose = writer2) {
            writer2.addAll(sampleIcebergrecords2);
        }

        DataFile dataFile2 = DataFiles.builder(sparkTable.table().spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out2.location())
                .withFileSizeInBytes(writer2.length())
                .withSplitOffsets(writer2.splitOffsets())
                .withMetrics(writer2.metrics())
                .build();

        LOGGER.debug("Table history size pre commit! {}", sparkTable.table().history().size());
        sparkTable.table().newOverwrite()
                .addFile(dataFile2)
                .commit();
        LOGGER.debug("Table history size post commit! {}", sparkTable.table().history().size());
        spark.sql("select * from default.iceberg_table").show();

        s3.listFiles();

        //spark.read().format("iceberg").load(sparkTable.table().location()+"#history").show(false);
        //spark.read().format("iceberg").load(sparkTable.table().location()+"#snapshots").show(false);
        //spark.read().format("iceberg").load(sparkTable.table().location()+"#manifests").show(false);
        //spark.read().format("iceberg").load(sparkTable.table().location()+"#files").show(false);
        // fails
        //spark.read().format("iceberg").load("default.iceberg_table").show(false);
        //spark.sql("SELECT * FROM default.iceberg_table.snapshots").show();
        //spark.table("default.iceberg_table.history").show();
        //spark.table("default.iceberg_table.snapshots").show();
        //spark.table("default.iceberg_table.manifests").show();
        //spark.table("default.iceberg_table.files").show();
        //spark.table("default.iceberg_table").show();
        //spark.sql("SELECT count(*) FROM default.iceberg_table").show();
    }

    public void runupsert() throws IOException, NoSuchTableException, TableAlreadyExistsException, NoSuchNamespaceException {
        // get catalog from spark
        SparkSessionCatalog sparkSessionCatalog = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
        Schema tableSchema = SparkSchemaUtil.convert(sampleDf.schema());
        Identifier tableIdentifier = Identifier.of(Namespace.of("default").levels(), "iceberg_table");
        Catalog hc = CatalogUtil.loadCatalog("org.apache.iceberg.hadoop.HadoopCatalog", "mycat", icebergOptions , spark.sparkContext().hadoopConfiguration());
        Map<String, String> tableProp = new ConcurrentHashMap<>();
        //tableProp.put(TableProperties.FORMAT_VERSION, "2");
        Table table = hc.createTable(TableIdentifier.of("default", "iceberg_table"), tableSchema,PartitionSpec.unpartitioned(), (Map)null);
        LOGGER.info("Iceberg Table schema is: {}", tableSchema.asStruct());
        // sparkSessionCatalog.createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
        // HadoopTables hadoopTables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());
        // Table hadoopTable = hadoopTables.load(tableIdentifier.name());

        sampleDf.writeTo("default.iceberg_table").using("iceberg").createOrReplace();
        spark.sql("select * from default.iceberg_table").show();
        spark.sql("INSERT INTO default.iceberg_table VALUES (10,'spark sql-insert')");
        SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
        LOGGER.debug("Spark table location: '{}'", sparkTable.table().location());

        // ---------- APPEND ----------
        OutputFile out = sparkTable.table().io().newOutputFile(sparkTable.table().locationProvider().newDataLocation(UUID.randomUUID()+"-001"));
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

        // ------------- DELETE ------
        OutputFile outDeletes = sparkTable.table().io().newOutputFile(sparkTable.table().locationProvider().newDataLocation(UUID.randomUUID()+"-001"));
        FileAppender<Record> writerDelete = Parquet.write(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(sparkTable.table())
                .overwrite()
                .build();
        try (Closeable toClose = writerDelete) {
            writerDelete.addAll(sampleIcebergrecords.subList(0,1));
        }

        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(sparkTable.table().spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(outDeletes.location())
                .withFileSizeInBytes(writerDelete.length())
                .withMetrics(writerDelete.metrics())
                .ofEqualityDeletes()
                .build();

        // Release 0.11
        //GenericAppenderFactory gaf = new GenericAppenderFactory(tableSchema).newAppender();
        //BaseTaskWriter tw = new BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
        //        OutputFileFactory fileFactory, FileIO io, long targetFileSize);
        LOGGER.debug("Table history size pre merge! {}", sparkTable.table().history().size());
        table.newRowDelta()
                .addRows(dataFile)
                .addDeletes(deleteFile)
                .commit();
        LOGGER.debug("Table history size post merge! {}", sparkTable.table().history().size());
        spark.sql("select * from default.iceberg_table").show();
        s3.listFiles();
    }

}
