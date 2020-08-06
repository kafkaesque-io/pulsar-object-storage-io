package com.kesque.pulsar.sink.s3.format.parquet;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.apache.logging.log4j.util.Strings;
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
    private Schema avroSchema;
    private volatile String currentFile = "";
    private volatile Record<byte[]> currentRecord; // kept for batch ack

    // parallel writer size
    int WRITER_LIMIT = 4;

    // key is the file name in S3
    private ConcurrentHashMap<String, ParquetWriter<GenericData.Record>> writerMap = new ConcurrentHashMap<String, ParquetWriter<GenericData.Record>>(WRITER_LIMIT); 
    private ConcurrentHashMap<String, S3ParquetOutputFile> s3ParquetOutputFileMap = new ConcurrentHashMap<String, S3ParquetOutputFile>(WRITER_LIMIT);

    // a thread pool of hard coded 4 threads for final commit and upload s3
    ExecutorService uploaderExecutor = Executors.newFixedThreadPool(WRITER_LIMIT);

    // S3ParquetOutputFile s3ParquetOutputFile = null;
    // private ParquetWriter<GenericData.Record> writer = null;

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
        JsonNode datum = JsonUtil.parse(convJson);
        this.avroSchema = JsonUtil.inferSchema(JsonUtil.parse(convJson), "schemafromjson");
        if (this.config.debugLoglevel()) {
            log.info(avroSchema.toString());
        }

        GenericData.Record convertedRecord = (org.apache.avro.generic.GenericData.Record) JsonUtil.convertToAvro(GenericData.get(), datum, avroSchema);
        writeParquet(convertedRecord, file);
        this.currentRecord = record;
    }

    private synchronized void writeParquet(GenericData.Record record, String file) {
        if (this.config.debugLoglevel()) {
            log.info("currentFile is {} file name is {}", this.currentFile, file);
        }
        String lastFile = this.currentFile; // save a copy because currentFile can be replace in the main thread
        Record<byte[]> lastRecord = this.currentRecord; // ditto save a copy
        if (Strings.isNotBlank(lastFile) && !file.equals(lastFile)) {
            uploaderExecutor.execute(() -> {
                ParquetWriter<GenericData.Record> writer = writerMap.get(lastFile);
                if (writer == null) {
                    log.error("fatal error - failed to find parquet writer to match file {}", lastFile);
                    return;
                }
                S3ParquetOutputFile s3ParquetOutputFile = s3ParquetOutputFileMap.get(lastFile);
                if (s3ParquetOutputFile == null) {
                    log.error("fatal error - failed to find s3ParquetOutputFile to match file {}", lastFile);
                    return;
                }

                // when a new file and parquet writer is required
                s3ParquetOutputFile.s3out.setCommit();
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("close parquet writer exception {}", e.getMessage());
                    e.printStackTrace();
                }
                writerMap.remove(lastFile);
                s3ParquetOutputFileMap.remove(lastFile);
                log.info("cumulative ack all pulsar messages and write to existing parquet writer, map size {}", writerMap.size());
                lastRecord.ack(); // depends on cumulative ack
            });
        }
        this.currentFile = file; // for the next write

        ParquetWriter<GenericData.Record> writer = this.writerMap.get(file);
        if (writer==null) {
            log.info("write to a new parquet writer with file {} currentFile {}", file, this.currentFile);
            S3ParquetOutputFile s3ParquetOutputFile = new S3ParquetOutputFile(this.s3Storage, file);

            try {
                writer = AvroParquetWriter.<GenericData.Record>builder(s3ParquetOutputFile).withSchema(avroSchema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY) // GZIP
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withConf(parquetWriterConfig)
                        .withPageSize(4 * 1024 * 1024) // For compression
                        .withRowGroupSize(16 * 1024 * 1024) // For write buffering (Page size)
                        .build();
            } catch (IOException e) {
                log.error("create parquet s3 writer exception {}", e.getMessage());
                e.printStackTrace();
            }

            s3ParquetOutputFileMap.put(file, s3ParquetOutputFile);
            writerMap.put(file, writer);
            log.info("put writer and parquet output file to {}", file);
        }

        try {
            writer.write(record);
        } catch (IOException e) {
            log.error("write to parquet s3 exception {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (!uploaderExecutor.isShutdown()) {
            uploaderExecutor.shutdown();
        }
        log.info("ParquetRecordWriter close()");
    }

    @Override
    public synchronized void commit() {
        log.info("ParquetRecordWriter commit()");
        if (Strings.isBlank(this.currentFile)) {
            return;
        }
        // TODO: reduce the synchronized block by only protect these two variables
        String lastFile = this.currentFile; // save a copy because currentFile can be replace in the main thread
        Record<byte[]> lastRecord = this.currentRecord; // ditto save a copy

        ParquetWriter<GenericData.Record> writer = writerMap.get(lastFile);
        if (writer == null) {
            log.error("fatal error - failed to find parquet writer to match file {}", lastFile);
            return;
        }
        S3ParquetOutputFile s3ParquetOutputFile = s3ParquetOutputFileMap.get(lastFile);
        if (s3ParquetOutputFile == null) {
            log.error("fatal error - failed to find s3ParquetOutputFile to match file {}", lastFile);
            return;
        }

        // when a new file and parquet writer is required
        s3ParquetOutputFile.s3out.setCommit();
        try {
            writer.close();
        } catch (IOException e) {
            log.error("close parquet writer exception {}", e.getMessage());
            e.printStackTrace();
        }
        writerMap.remove(lastFile);
        s3ParquetOutputFileMap.remove(lastFile);
        log.info("cumulative ack all pulsar messages and write to existing parquet writer, map size {}", writerMap.size());
        lastRecord.ack(); // depends on cumulative ack

        this.currentFile = "";
    }
}