package com.kesque.pulsar.sink.s3.format;

import java.io.Closeable;

import org.apache.pulsar.functions.api.Record;

/**
 * Record format for object storage.
 */
public interface RecordWriter extends Closeable {
    /**
     * Write a record to storage.
     *
     * @param record the record to persist.
     */
    void write(Record<byte[]> record, String file);

    /**
     * Close this writer.
     */
    void close();

    /**
     * Flush writer's data and commit the records in Kafka. Optionally, this operation might also
     * close the writer.
     */
    void commit();
    
}