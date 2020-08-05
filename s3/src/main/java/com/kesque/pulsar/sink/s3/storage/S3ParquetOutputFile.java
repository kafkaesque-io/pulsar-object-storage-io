package com.kesque.pulsar.sink.s3.storage;

import java.io.IOException;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class S3ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;
    private S3Storage storage;
    private String filename;
    public S3ParquetOutputStream s3out;

    public S3ParquetOutputFile(S3Storage storage, String filename) {
        this.storage = storage;
        this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        s3out = (S3ParquetOutputStream) storage.create(filename, true);
        return s3out;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
    
}