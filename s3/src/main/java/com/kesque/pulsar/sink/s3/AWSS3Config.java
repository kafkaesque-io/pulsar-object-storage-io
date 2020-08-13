package com.kesque.pulsar.sink.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.SdkClientException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.kesque.pulsar.sink.s3.storage.CompressionType;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.Serializable;
import java.io.IOException;
import java.util.Map;

public class AWSS3Config implements Serializable {

    private static final long serialVersionUID = 1L;

    //public static final String ACCESS_KEY_NAME = "accessKey";
    //public static final String SECRET_KEY_NAME = "secretKey";

    public int S3RetryBackoffConfig = 100; // ms
    public int S3PartRetries = 4;

    public String endpointURL = "";

    private String accessKeyId = null;
    public String getAccessKeyId() {
        return this.accessKeyId;
    }
    public void setAccessKeyId(String keyId) {
        this.accessKeyId = keyId;
    }

    private String secretAccessKey = null;
    public String getSecretAccessKey() {
        return this.secretAccessKey;
    }
    public void setSecretAccessKey(String secretKey) {
        this.secretAccessKey = secretKey;
    }

    // a timer interval for s3 Object rollover in minutes
    private int s3ObjectRolloverMinutes = 10;
    public int getS3ObjectRolloverMinutes() {
        return this.s3ObjectRolloverMinutes;
    }
    public void setS3ObjectRolloverMinutes(int s3ObjectRolloverMinutes) {
        if (s3ObjectRolloverMinutes>0) {
            this.s3ObjectRolloverMinutes = s3ObjectRolloverMinutes;
        }
    }

    private String type = "json-to-parquet";
    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return this.type;
    }

    private boolean topicSchemaRequired = false;
    public void setTopicSchemaRequired(boolean topicSchemaRequired) {
        this.topicSchemaRequired = topicSchemaRequired;
    }
    public boolean getTopicSchemaRequired() {
        return this.topicSchemaRequired;
    }

    private boolean isDebug = false;
    public boolean debugLoglevel() {
        return isDebug;
    }

    // currently only support debug level
    private String logLevel = "";
    public String getLogLevel() {
        return this.logLevel;
    }
    /**
     * @param logLevel the logLevel to set
     */
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        this.isDebug = this.logLevel.equalsIgnoreCase("debug");
    }

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Specific aws region. E.g. us-east-1, us-west-2"
    )
    private String awsregion = "";
    public String getAWSRegion() {
        return this.awsregion;
    }
    public void setAWSRegion(String region) {
        this.awsregion = region;
    }

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "AWS S3 bucket name"
    )
    private String bucketName = "";
    public String getBucketName() {
        return this.bucketName;
    }
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public int partSize = 5 * 1024 * 1024;
    public int getPartSize() {
        return this.partSize;
    }
    public void setPartSize(int partSize) {
        this.partSize = partSize;
    }

    public int compressionLevel = 1; // from -1 to 9

    public CompressionType compressionType = CompressionType.NONE;

    public static AWSS3Config load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), AWSS3Config.class);
    }

    public static AWSS3Config load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), AWSS3Config.class);
    }
    
    public AmazonS3 newS3Client() {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withAccelerateModeEnabled(true) //config.getBoolean(WAN_MODE_CONFIG))
        .withPathStyleAccessEnabled(true)
        .withCredentials(newCredentialsProvider());
        // .withClientConfiguration(clientConfiguration);

        String region = this.awsregion;
        if (StringUtils.isBlank(this.endpointURL)) {
            builder = "us-east-1".equals(region)
                    ? builder.withRegion(Regions.US_EAST_1)
                    : builder.withRegion(region);
        } else {
            builder = builder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(this.endpointURL, region)
            );
        }

        return builder.build();
    }

    public AWSCredentialsProvider newCredentialsProvider() {
        BasicAWSCredentials basicCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        return new AWSStaticCredentialsProvider(basicCredentials);
    }
}