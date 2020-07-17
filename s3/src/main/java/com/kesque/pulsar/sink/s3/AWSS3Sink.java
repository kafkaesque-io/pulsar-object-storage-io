package com.kesque.pulsar.sink.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.Lists;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.functions.api.Record;
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

    private AWSS3Config s3Config;
    private String bucketName;
    
    private int fileSizeBytes = 10 * 1024 * 1024;

    private int bufferBytes = 0;
    private List<Record<byte[]>> incomingList;
    private ScheduledExecutorService flushExecutor;

    private AWSS3Uploader uploader;

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
        }
        if (bufferBytes > fileSizeBytes) {
            flushExecutor.submit(() -> flush());
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Kinesis sink stopped.");
        uploader.shutdown();
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
        incomingList = Lists.newArrayList();

        flushExecutor = Executors.newScheduledThreadPool(1);
    }

    private void flush() {
        final Map<byte[], byte[]> recordsToSet = new HashMap<>();
        final List<Record<byte[]>> recordsToFlush;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            recordsToFlush = incomingList;
            incomingList = Lists.newArrayList();
        }

        byte[] data = {};
        if (CollectionUtils.isNotEmpty(recordsToFlush)) {
            for (Record<byte[]> record: recordsToFlush) {
                try {
                    // records with null keys or values will be ignored
                    byte[] key = record.getKey().isPresent() ? record.getKey().get().getBytes(StandardCharsets.UTF_8) : null;
                    byte[] value = record.getValue();
                    recordsToSet.put(key, value);
                    data = Bytes.concat(data, value);
                } catch (Exception e) {
                    record.fail();
                    recordsToFlush.remove(record);
                    log.warn("Record flush thread was exception ", e);
                }
            }
        }

        try {
            if (recordsToSet.size() > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Calling mset with {} values", recordsToSet.size());
                }

                String keyName = "";
                uploader.upload(keyName, data);
                
            }
            recordsToFlush.forEach(tRecord -> tRecord.ack());
            recordsToSet.clear();
            recordsToFlush.clear();
        } catch (Exception e) {
            recordsToFlush.forEach(tRecord -> tRecord.fail());
            log.error("upload data interrupted exception ", e);
        }
    }
}