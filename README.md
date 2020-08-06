# Pulsar AWS S3 sink

Pulsar AWS S3 sink receives JSON messages over Pulsar topics with the same schema and saves them as Parquet format on AWS S3.

## Operations
### Deployment
Copy the nar file to ./pulsar/connectors directory

GET `admin/v2/functions/connectors` displays the nar is loaded successfully as
```
{"name":"aws-s3","description":"AWS S3 sink","sinkClass":"com.kesque.pulsar.sink.s3.AWSS3Sink"}
```

`EFFECTIVELY_ONCE` processing guarantees is required since it implements message [cumulative acknowledgement](https://github.com/apache/pulsar/blob/master/pulsar-functions/instance/src/main/java/org/apache/pulsar/functions/source/PulsarSource.java#L129)

Create a sink by uploading a nar file.
```
$ bin/pulsar-admin sinks create --archive ./connectors/pulsar-io-s3-1.0.nar --inputs aws-s3-input-topic --name aws-s3-test --sink-config-file ./connectors/pulsar-s3-io.yaml --processing-guarantees EFFECTIVELY_ONCE --subs-position Earliest --sub-name auniquename
"Created successfully"
```

Create a sink from a preloaded nar file.
```
$ bin/pulsar-admin sinks create --sink-type aws-s3 --inputs aws-s3-input-topic --name aws-s3-test --sink-config-file ./connectors/pulsar-s3-io.yaml --processing-guarantees EFFECTIVELY_ONCE --subs-position Earliest --subs-name auniquename
"Created successfully"

$ bin/pulsar-admin sinks list
[
  "aws-s3-test"
]

$ bin/pulsar-admin sinks delete --name aws-s3-test 
"Deleted successfully"
```

### Topic schema registry
It is mandatory a schema is enforced over the input topics. The Sink would have fatal error to create parquet format when it receives messages with different schemas.

## Build
The command to build a nar file.
```
$ cd s3
$ mvn clean install
```
