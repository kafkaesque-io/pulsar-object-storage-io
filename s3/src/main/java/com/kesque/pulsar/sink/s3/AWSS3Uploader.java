package com.kesque.pulsar.sink.s3;

import java.io.ByteArrayInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.Upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.SdkClientException;

public class AWSS3Uploader extends AWSS3TransferManager {

    private static final Logger log = LoggerFactory.getLogger(AmazonS3Util.class);

    public AWSS3Uploader(String accessKeyId, String secretAccessKey, String region, String bucket) {
        super(accessKeyId, secretAccessKey, region, bucket);
    }

    public void upload(String keyName, byte[] data) throws Exception {

        String logPrefix = "file key name " + keyName + " under bucket " + bucketName;
        try {
            // TransferManager processes all transfers asynchronously,
            int contentLength = data.length;
            ByteArrayInputStream bytes = new ByteArrayInputStream(data);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(contentLength);
            Upload upload = transferManager.upload(this.bucketName, keyName, bytes, objectMetadata);

            log.info("Object upload started to " + logPrefix);

            // Optionally, wait for the upload to finish before continuing.
            upload.waitForCompletion();
            log.info("Object upload complete to " + logPrefix);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
            log.error("Object upload to " + logPrefix + " AmazonServiceException - " + e.getErrorMessage());
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
            log.error("Object upload to " + logPrefix + " SdkClientException - " + e.getMessage());
        }
    }
}