# Pulsar AWS S3 sink

Pulsar AWS S3 sink

## Operations
### Deployment
Copy the nar file to ./pulsar/connectors directory

GET `admin/v2/functions/connectors` displays the nar is loaded successfully as
```
{"name":"aws-s3","description":"AWS S3 sink","sinkClass":"com.kesque.pulsar.sink.s3.AWSS3Sink"}
```

```
$ bin/pulsar-admin sinks create --archive ./connectors/pulsar-io-s3-1.0.nar --inputs aws-s3-input-topic --name aws-s3-test --sink-config-file ./connectors/pulsar-postgres-jdbc-sink.yaml
"Created successfully"

$ bin/pulsar-admin sinks list
[
  "aws-s3-test"
]

$ bin/pulsar-admin sinks delete --name aws-s3-test 
"Deleted successfully"
```

## Build
The command to build a nar file.
```
$ cd s3
$ mvn clean install
```
