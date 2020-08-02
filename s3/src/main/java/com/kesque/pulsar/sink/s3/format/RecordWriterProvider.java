package com.kesque.pulsar.sink.s3.format;

import com.kesque.pulsar.sink.s3.AWSS3Config;
import com.kesque.pulsar.sink.s3.format.parquet.ParquetRecordWriter;
import com.kesque.pulsar.sink.s3.storage.S3Storage;

public class RecordWriterProvider {
    public static ParquetRecordWriter createParquetRecordWriter(AWSS3Config confg, S3Storage storage) {
        return new ParquetRecordWriter(confg, storage); 

    }
}