package com.kesque.pulsar.sink.s3.format.parquet;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.kesque.pulsar.sink.s3.AWSS3Config;
import com.kesque.pulsar.sink.s3.format.RecordWriter;
import com.kesque.pulsar.sink.s3.storage.S3ParquetOutputFile;
import com.kesque.pulsar.sink.s3.storage.S3Storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRecordWriter implements RecordWriter {

    private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriter.class);
    private static final String EXTENSION = ".parquet";
    private static final int PAGE_SIZE = 64 * 1024;
    private AWSS3Config config;
    private S3Storage s3Storage;
    private Configuration parquetWriterConfig;
    private String currentFile = "";
    private Schema avroSchema;
    private Record<byte[]> record; // kept for batch ack

    S3ParquetOutputFile s3ParquetOutputFile = null;
    private ParquetWriter<GenericData.Record> writer = null;

    public ParquetRecordWriter(AWSS3Config confg, S3Storage storage) {
        this.config = confg;
        this.s3Storage = storage;

        parquetWriterConfig = new Configuration();
        parquetWriterConfig.set("fs.s3.awsAccessKeyId", config.getAccessKeyId());
        parquetWriterConfig.set("fs.s3.awsSecretAccessKey", config.getSecretAccessKey());

    }

    @Override
    public void write(Record<byte[]> record, String file) {
        byte[] data = record.getValue();
        String convJson = new String(data); // StandardCharsets.UTF_8);
        log.info("data payload length is {} string-value {}", data.length);
        JsonNode datum = JsonUtil.parse(convJson);
        this.avroSchema = JsonUtil.inferSchema(JsonUtil.parse(convJson), "schemafromjson");
        log.info(avroSchema.toString());

        GenericData.Record convertedRecord = (org.apache.avro.generic.GenericData.Record) JsonUtil.convertToAvro(GenericData.get(), datum, avroSchema);

        try {
            if (file.equals(currentFile)) {
                log.info("write to existing parquet writer");
                writer.write(convertedRecord);

            } else {
                this.currentFile = file; 
                if (this.writer != null) {
                    writer.write(convertedRecord);
                    log.info("cumulative ack all pulsar messages and write to existing parquet writer");
                    record.ack(); // depends on cumulative ack
                    s3ParquetOutputFile.s3out.setCommit();
                    this.writer.close();
                    this.writer = null;
                } else {
                    s3ParquetOutputFile = new S3ParquetOutputFile(this.s3Storage, file);

                    log.info("write to a new parquet writer");
                
                    this.writer = AvroParquetWriter.<GenericData.Record>builder(s3ParquetOutputFile).withSchema(avroSchema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY) // GZIP
                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withConf(parquetWriterConfig)
                            .withPageSize(4 * 1024 * 1024) // For compression
                            .withRowGroupSize(16 * 1024 * 1024) // For write buffering (Page size)
                            .build();
                    
                    writer.write(convertedRecord);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("write to parquet s3 exception {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        log.info("ParquetRecordWriter close()");
    }

    @Override
    public void commit() {
        log.info("ParquetRecordWriter commit()");
    }
    
}