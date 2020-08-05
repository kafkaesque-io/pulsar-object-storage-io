package com.kesque.pulsar.sink.s3.storage;

import java.io.IOException;

import com.amazonaws.services.s3.AmazonS3;
import com.kesque.pulsar.sink.s3.AWSS3Config;

public class S3ParquetOutputStream extends S3OutputStream {

  private volatile boolean commit;

  public S3ParquetOutputStream(String key, AWSS3Config conf, AmazonS3 s3) {
    super(key, conf, s3);
    commit = false;
  }

  @Override
  public void close() throws IOException {
    if (commit) {
      super.commit();
      commit = false;
    } else {
      super.close();
    }
  }

  public void setCommit() {
    commit = true;
  }
}