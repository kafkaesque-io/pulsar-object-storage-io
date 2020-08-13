package com.kesque.pulsar.sink.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import com.kesque.pulsar.sink.s3.format.RecordWriterProvider;
import com.kesque.pulsar.sink.s3.format.parquet.ParquetRecordWriter;
import com.kesque.pulsar.sink.s3.storage.S3Storage;

import org.apache.avro.SchemaParseException;
import org.apache.avro.reflect.AvroSchema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.inferred.freebuilder.shaded.com.google.common.primitives.Bytes;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an AWS S3 sink that receives JSON message and store them
 * Apache Parquet format in AWS S3.
 */
@Connector(
    name = "aws-s3",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to AWS S3.",
    configClass = AWSS3Config.class
)
public class AWSS3Sink implements Sink<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(AWSS3Sink.class);

    private AWSS3Config s3Config;
    private String bucketName;
    private String filePrefix = "";

    private int fileSizeBytes = 10 * 1024 * 1024;

    private ScheduledExecutorService s3RolloverExecutor;

    private SchemaInfo schemaInfo;
    private org.apache.avro.Schema avroSchema;

    private boolean validateTopicSchema = false;

    private ParquetRecordWriter recordWriter;

    private volatile String filename;

    private long lastRecordEpoch = 0;

    private long s3ObjectRolloverMinutes = 10;
    private long MILLIS_IN_MINUTE = 60 * 1000;

    /**
     * Write a message to Sink
     * 
     * @param inputRecordContext Context of input record from the source
     * @param record             record to write to sink
     * @throws Exception
     */
    @Override
    public void write(Record<byte[]> record) throws Exception {
        if (validateTopicSchema) {
            org.apache.pulsar.client.api.Schema<byte[]> localSchema = record.getSchema();
            if (localSchema == null) {
                log.error("schema has not set up against the topic ");
            }
            SchemaInfo info = localSchema.getSchemaInfo();
            log.info("schema type {} schemadefinition {}", info.getType(), info.getSchemaDefinition());
            // throws exception if schema cannot be retrieved
        }
        synchronized (this) {
            this.lastRecordEpoch = Util.getNowMilli();
            Long ledgerId = getLedgerId(record.getRecordSequence().get());
            if (this.s3Config.debugLoglevel()) {
                log.info("ledgerID {} event time {}", ledgerId, this.lastRecordEpoch);
            }
            // Optional<Message<byte[]>> msgOption = record.getMessage(); //.get();
            // log.error("message option isPresent {}", msgOption.isPresent());

            this.filename = getFilename(this.filePrefix, ledgerId);
            this.recordWriter.write(record, this.filename);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("s3 sink stopped...");
        s3RolloverExecutor.shutdown();
    }

    /**
    * Open connector with configuration
    *
    * @param config initialization config
    * @param sinkContext
    * @throws Exception IO type exceptions when opening a connector
    */
    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("open aws s3 sink configs size {}", config.size());
        s3Config = AWSS3Config.load(config);

        validateTopicSchema = s3Config.getTopicSchemaRequired();
        bucketName = s3Config.getBucketName();
        for (String topicName : sinkContext.getInputTopics()){
            filePrefix = topicName + "-" + filePrefix;
        }
        log.info("filePrefix is " + this.filePrefix);

        S3Storage storage = new S3Storage(this.s3Config, "");
        this.recordWriter = RecordWriterProvider.createParquetRecordWriter(s3Config, storage);

        this.s3ObjectRolloverMinutes = s3Config.getS3ObjectRolloverMinutes();
        log.info("s3 object rollover interval {} minutes", this.s3ObjectRolloverMinutes);
        s3RolloverExecutor = Executors.newScheduledThreadPool(1);
        s3RolloverExecutor.scheduleAtFixedRate(() -> triggerS3ObjectRollover(), 0, s3Config.getS3ObjectRolloverMinutes(), TimeUnit.MINUTES);
    }

    public static long getLedgerId(long sequenceId) {
        return sequenceId >>> 28;
    }

    private boolean isAvroSchema(SchemaData schemaData) {
        try {
            org.apache.avro.Schema.Parser fromParser = new org.apache.avro.Schema.Parser();
            fromParser.setValidateDefaults(false);
            org.apache.avro.Schema fromSchema = fromParser.parse(new String(schemaData.getData(), UTF_8));
            return true;
        } catch (SchemaParseException e) {
            return false;
        }
    }

    private static String getFilename(String prefix, Long ledger) {
        return prefix + Long.toString(ledger);
    }

    private void triggerS3ObjectRollover() {
        if (this.lastRecordEpoch == 0) {
            return;
        }

        if (MILLIS_IN_MINUTE * s3ObjectRolloverMinutes < (Util.getNowMilli() - this.lastRecordEpoch)) {
            this.recordWriter.commit();
        }
    }
}