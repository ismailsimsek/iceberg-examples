import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergJavaApiAppenderFactory extends Setup {


    ArrayList<Record> sampleIcebergrecords = Lists.newArrayList();
    ArrayList<Record> sampleIcebergrecords2 = Lists.newArrayList();

    public IcebergJavaApiAppenderFactory() throws Exception {
        super();
        GenericRecord record = GenericRecord.create(SparkSchemaUtil.convert(sampleDf.schema()));
        sampleIcebergrecords.add(record.copy("age", 29L, "name", "GenericRecord-a"));
        sampleIcebergrecords.add(record.copy("age", 43L, "name", "GenericRecord-b"));

        sampleIcebergrecords2.add(record.copy("age", 129L, "name", "GenericRecord-2-c"));
        sampleIcebergrecords2.add(record.copy("age", 123L, "name", "GenericRecord-2-d"));

    }

    public static void main(String[] args) throws Exception {
        IcebergJavaApiAppenderFactory myExample = new IcebergJavaApiAppenderFactory();
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
        Table icebergTable = sparkTable.table();
        String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        FileFormat fileFormat = FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));

        LOGGER.debug("Creating iceberg DataFile with GenericAppenderFactory!");
        GenericAppenderFactory appender = new GenericAppenderFactory(
                icebergTable.schema(),
                icebergTable.spec(),
                Ints.toArray(icebergTable.schema().identifierFieldIds()),
                icebergTable.schema(),
                null);
        final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + fileFormat.name();
        OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));
        DataWriter<Record> dw = appender.newDataWriter(icebergTable.encryption().encrypt(out), fileFormat, null);
        sampleIcebergrecords.forEach(dw::write);
        try {
            dw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataFile dataFile = dw.toDataFile();
        sparkTable.table().newAppend()
                .appendFile(dataFile)
                .commit();
        LOGGER.warn("------------AFTER API APPEND----------------");
        spark.sql("select * from default.iceberg_table").show();
        LOGGER.warn("------------FINAL S3 FILE LIST ----------------");
        s3.listFiles();

    }

}
