# Pulsar Cassandra Astra sink

Pulsar Cassandra Astra sink receives JSON messages over Pulsar topics with the same schema and add a new row per Pulsar message in Datastax Astra's Cassandra database. 

## Operations
### Deployment
Copy the nar file to ./pulsar/connectors directory

GET `admin/v2/functions/connectors` displays the nar is loaded successfully as
```
{"name":"astra-cassandra","description":"write data to cassandra astra","sinkClass":"com.kesque.pulsar.sink.cassandra.astra.AstraSink"}
```

Create a sink from a preloaded nar file.
```
$ bin/pulsar-admin sinks create --sink-type "astra-cassandra" --inputs astra-input-topic --name astra-test --sink-config-file ./connectors/pulsar-astra-sink.yaml --processing-guarantees EFFECTIVELY_ONCE --subs-position Earliest --subs-name auniquename
"Created successfully"

$ bin/pulsar-admin sinks list
[
  "astra-test"
]

$ bin/pulsar-admin sinks delete --name astra-test 
"Deleted successfully"
```

### Sink configuration
Astra sink configuration requires these parameters to generate auth token.
```
ASTRA_CLUSTER_ID=
ASTRA_CLUSTER_REGION=
ASTRA_DB_USERNAME=
ASTRA_DB_PASSWORD=
```
Additionally, `keyspace` and `table name` are mandatory to add rows into Astra Cassandra database.

`table schema` is optional. However, the table schema has to be created on the table before any row can be added if the schema is not specified in the configuration.

These configuration must be specifed in [the configuration yaml](./config/pulsar-astra-sink.yaml)

When set `logLevel: "debug"`, debug logs will be printed by the sink.

### Topic schema registry
It is mandatory a schema is enforced over the input topics. The Sink would have fatal error to create parquet format when it receives messages with different schemas.

## Build
The command to build a nar file.
```
$ cd cassandra-astra
$ mvn clean install
```
