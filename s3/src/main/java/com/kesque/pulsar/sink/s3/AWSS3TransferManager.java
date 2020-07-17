package com.kesque.pulsar.sink.s3;

import java.util.concurrent.Executors;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public abstract class AWSS3TransferManager {
    
    String bucketName = "";

    long multipartUploadThreshold = 5 * 20124 * 1024;
    int maxUploadThreads=2;

    TransferManager transferManager;

    public AWSS3TransferManager (String accessKeyId, String secretAccessKey, String region, String bucket) {
        this.bucketName = bucket;

        BasicAWSCredentials basicCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        AWSStaticCredentialsProvider cred = new AWSStaticCredentialsProvider(basicCredentials);    
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(region)
            .withCredentials(cred)
            .build();
        this.transferManager = TransferManagerBuilder.standard()
            .withS3Client(s3Client)
            .withMultipartUploadThreshold(multipartUploadThreshold)
            .withExecutorFactory(() -> Executors.newFixedThreadPool(maxUploadThreads))
            .build();
        s3Client.getRegionName();
    }

    public void setMaxUploadThreads(int maxUploadThread) {
        this.maxUploadThreads = maxUploadThread;
    }
    
    public void setMultipartUploadThreshold(long minMultipartUploadPartSize) {
        this.multipartUploadThreshold = minMultipartUploadPartSize;
    }

    public String getBucketName() {
        return this.bucketName;
    }
    
    public void shutdown() {
        if (this.transferManager != null) {
            this.transferManager.shutdownNow();
        }
    }
}