package com.kesque.pulsar.sink.s3.storage;

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
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Strings;
import com.kesque.pulsar.sink.s3.AWSS3Config;
import com.amazonaws.SdkClientException;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.Deflater;

public class S3Storage {
    private static final Logger log = LoggerFactory.getLogger(S3Storage.class);

    private final String VERSION = "0.1";

    private final String url;
    private final String bucketName;
    private final AmazonS3 s3;

    private final AWSS3Config conf;
    private static final String VERSION_FORMAT = "APN/1.0 Kesque/1.0 KesqueS3Sink/%s";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_ACCESS_KEY_ID_DEFAULT = "";

    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    // public static final Password AWS_SECRET_ACCESS_KEY_DEFAULT = new Password(null);

    public static final String REGION_CONFIG = "s3.region";
    public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

    public static final String ACL_CANNED_CONFIG = "s3.acl.canned";
    public static final String ACL_CANNED_DEFAULT = null;

    public static final String COMPRESSION_TYPE_CONFIG = "s3.compression.type";
    public static final String COMPRESSION_TYPE_DEFAULT = "none";

    public static final String COMPRESSION_LEVEL_CONFIG = "s3.compression.level";
    public static final int COMPRESSION_LEVEL_DEFAULT = Deflater.DEFAULT_COMPRESSION;
    //private static final CompressionLevelValidator COMPRESSION_LEVEL_VALIDATOR =
    //    new CompressionLevelValidator();

    public static final String S3_PART_RETRIES_CONFIG = "s3.part.retries";
    public static final int S3_PART_RETRIES_DEFAULT = 3;

    public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
    public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

    public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
    public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();

    public static final String S3_PROXY_URL_CONFIG = "s3.proxy.url";
    public static final String S3_PROXY_URL_DEFAULT = "";

    public static final String S3_PROXY_USER_CONFIG = "s3.proxy.user";
    public static final String S3_PROXY_USER_DEFAULT = null;

    public static final String S3_PROXY_PASS_CONFIG = "s3.proxy.password";
    // public static final Password S3_PROXY_PASS_DEFAULT = new Password(null);

    public static final String HEADERS_USE_EXPECT_CONTINUE_CONFIG =
        "s3.http.send.expect.continue";
    public static final boolean HEADERS_USE_EXPECT_CONTINUE_DEFAULT =
        ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE;

    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    // public static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.FAIL.toString();

    public static final int S3_RETRY_MAX_BACKOFF_TIME_MS = (int) TimeUnit.HOURS.toMillis(24);

    public static final String S3_RETRY_BACKOFF_CONFIG = "s3.retry.backoff.ms";
    public static final int S3_RETRY_BACKOFF_DEFAULT = 200;

    long multipartUploadThreshold = 5 * 20124 * 1024;
    int maxUploadThreads=2;
    
    public S3Storage(AWSS3Config conf, String url) {
        this.url = url;
        this.conf = conf;
        this.bucketName = conf.getBucketName();
        this.s3 = newS3Client(conf);
    }
    
    public AmazonS3 newS3Client(AWSS3Config config) {
        ClientConfiguration clientConfiguration = newClientConfiguration(config);
        BasicAWSCredentials basicCredentials = new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey());
        AWSStaticCredentialsProvider cred = new AWSStaticCredentialsProvider(basicCredentials);    
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
            // .withAccelerateModeEnabled(true) //config.getBoolean(WAN_MODE_CONFIG))
            // .withPathStyleAccessEnabled(true)
            // .withCredentials(newCredentialsProvider(config)) // TODO Fix 
            .withCredentials(cred)
            .withClientConfiguration(clientConfiguration);
    
        String region = config.getAWSRegion(); //.getString(REGION_CONFIG);
        if (Strings.isNullOrEmpty(url)) {
          builder = "us-east-1".equals(region)
                    ? builder.withRegion(Regions.US_EAST_1)
                    : builder.withRegion(region);
        } else {
          builder = builder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(url, region)
          );
        }
    
        return builder.build();
    } 

    public TransferManager newS3TransferManager() {
        return TransferManagerBuilder.standard()
            .withS3Client(this.s3)
            .withMultipartUploadThreshold(multipartUploadThreshold)
            .withExecutorFactory(() -> Executors.newFixedThreadPool(maxUploadThreads))
            .build();
    }

    public ClientConfiguration newClientConfiguration(AWSS3Config config) {
        String version = String.format(VERSION_FORMAT, VERSION);
    
        ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
        clientConfiguration.withUserAgentPrefix(version)
            .withRetryPolicy(newFullJitterRetryPolicy(config));
        /* if (Strings.isNullOrEmpty(config.getString(S3_PROXY_URL_CONFIG))) {
          S3ProxyConfig proxyConfig = new S3ProxyConfig(config);
          clientConfiguration.withProtocol(proxyConfig.protocol())
              .withProxyHost(proxyConfig.host())
              .withProxyPort(proxyConfig.port())
              .withProxyUsername(proxyConfig.user())
              .withProxyPassword(proxyConfig.pass());
        }
        clientConfiguration.withUseExpectContinue(config.useExpectContinue());
        */
    
        return clientConfiguration;
      }

    protected RetryPolicy newFullJitterRetryPolicy(AWSS3Config config) {
        PredefinedBackoffStrategies.FullJitterBackoffStrategy backoffStrategy =
            new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
                config.S3RetryBackoffConfig,
                S3_RETRY_MAX_BACKOFF_TIME_MS
            );
    
        RetryPolicy retryPolicy = new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
            backoffStrategy,
            conf.S3PartRetries,
            false
        );
        return retryPolicy;
    }

    public S3OutputStream create(String path, boolean overwrite) {
        if (!overwrite) {
          throw new UnsupportedOperationException(
              "Creating a file without overwriting is not currently supported in S3 Connector"
          );
        }
    
        if (Strings.isNullOrEmpty(path)) {
          throw new IllegalArgumentException("Path can not be empty!");
        }
    
        System.out.println("S3OutputStream . create() ... ");
        return new S3ParquetOutputStream(path, this.conf, s3);
    }
}