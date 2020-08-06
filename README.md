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

Option `--subs-name` is required too.

Create a sink by uploading a nar file.
```
$ bin/pulsar-admin sinks create --archive ./connectors/pulsar-io-s3-1.0.nar --inputs aws-s3-input-topic --name aws-s3-test --sink-config-file ./connectors/pulsar-s3-io.yaml --processing-guarantees EFFECTIVELY_ONCE --subs-position Earliest --subs-name auniquename
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

### Sink configuration
AWS S3 configuration such as accessKeyId, secretAccessKey, region, and bucketname needs to be specifed in [the configuration yaml](./s3/config/pulsar-s3-io.yaml)

When set `logLevel: "debug"`, debug logs will be printed by the sink.

Pulsar messages under the same ledger ID are grouped under a single S3 Object file. S3 object file follows the naming convention prefix with the input topic with the ledger Id. All messages are under the same ledger Id are written this file.

Since the sink uses the latest Pulsar message's ledger ID to detect the ledger rollover, a time based S3 Object rollover is also required to write the last ledger's messages into S3 in the case the messages over an topic are permanently stopped. `s3ObjectRolloverMinutes` in the config must be greater than `managedLedgerMaxLedgerRolloverTimeMinutes` set up in the Pulsar's broker.conf.

Because of ledger Id is used to identify an S3 object, the sink currently only supports a single input topic.

How Pulsar managed the ledger is configurable. Here are the default settings (from broker.conf):
```
# Max number of entries to append to a ledger before triggering a rollover
# A ledger rollover is triggered on these conditions
#  * Either the max rollover time has been reached
#  * or max entries have been written to the ledged and at least min-time
#    has passed
managedLedgerMaxEntriesPerLedger=50000

# Minimum time between ledger rollover for a topic
managedLedgerMinLedgerRolloverTimeMinutes=10

# Maximum time before forcing a ledger rollover for a topic
managedLedgerMaxLedgerRolloverTimeMinutes=240
```

### Topic schema registry
It is mandatory a schema is enforced over the input topics. The Sink would have fatal error to create parquet format when it receives messages with different schemas.

## Build
The command to build a nar file.
```
$ cd s3
$ mvn clean install
```
