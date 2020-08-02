package com.kesque.pulsar.sink.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import com.kesque.pulsar.sink.s3.format.RecordWriterProvider;
import com.kesque.pulsar.sink.s3.format.parquet.ParquetRecordWriter;
import com.kesque.pulsar.sink.s3.storage.S3Storage;

import org.apache.avro.SchemaParseException;
import org.apache.avro.reflect.AvroSchema;
import org.apache.commons.collections4.CollectionUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Simple Redis sink, which stores the key/value records from Pulsar in redis.
 * Note that records from Pulsar with null keys or values will be ignored.
 * This class expects records from Pulsar to have a key and value that are stored as bytes or a string.
 */
@Connector(
    name = "aws-s3",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to AWS S3.",
    configClass = AWSS3Config.class
)
public class AWSS3Sink implements Sink<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(AWSS3Sink.class);

    private static final String MessageIdImpl = null;

    private AWSS3Config s3Config;
    private String bucketName;
    private String filePrefix = "";
    
    private int fileSizeBytes = 10 * 1024 * 1024;

    private int bufferBytes = 0;
    private List<Record<byte[]>> incomingList;
    private ScheduledExecutorService flushExecutor;

    private SchemaInfo schemaInfo;
    private org.apache.avro.Schema avroSchema;

    private AmazonS3 s3Client;
    private ParquetRecordWriter recordWriter;

    private String filename;

    /**
    * Write a message to Sink
    * @param inputRecordContext Context of input record from the source
    * @param record record to write to sink
    * @throws Exception
    */
    @Override
    public void write(Record<byte[]> record) throws Exception {
        synchronized (this) {
            bufferBytes += record.getValue().length;
            incomingList.add(record);
            //long epoch = record.getEventTime().get();

            // build avro schema for parquet format
            this.schemaInfo = record.getSchema().getSchemaInfo();
            this.avroSchema = new org.apache.avro.Schema.Parser().parse(
               new String(this.schemaInfo.getSchema(), UTF_8)
            );
            
            //String key = record.getKey().get(); // key is for file name
            Long ledgerId = getLedgerId(record.getRecordSequence().get());

            this.filename = getFilename(this.filePrefix, ledgerId);
        }

        //bytes to generic data ??
    }

    @Override
    public void close() throws IOException {
        log.info("s3 sink stopped...");
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
        log.info("Open AWS S3 sink");
        s3Config = AWSS3Config.load(config);

        bucketName = s3Config.getBucketName();
        for (String topicName : sinkContext.getInputTopics()){
            filePrefix = topicName + "-" + filePrefix;
        }

        incomingList = Lists.newArrayList();

        flushExecutor = Executors.newScheduledThreadPool(1);

        this.s3Client = s3Config.newS3Client();

        S3Storage storage = new S3Storage(this.s3Config, "");
        this.recordWriter = RecordWriterProvider.createParquetRecordWriter(s3Config, storage);
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
}