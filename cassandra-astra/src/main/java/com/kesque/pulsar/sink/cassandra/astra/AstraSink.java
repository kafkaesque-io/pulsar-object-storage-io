package com.kesque.pulsar.sink.cassandra.astra;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.avro.SchemaParseException;
import org.apache.avro.reflect.AvroSchema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.inferred.freebuilder.shaded.com.google.common.primitives.Bytes;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.JsonNode;
import kong.unirest.json.JSONObject;

/**
 * A Simple Redis sink, which stores the key/value records from Pulsar in redis.
 * Note that records from Pulsar with null keys or values will be ignored.
 * This class expects records from Pulsar to have a key and value that are stored as bytes or a string.
 */
@Connector(
    name = "cassandra-astra",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to Cassandra Astra.",
    configClass = AstraConfig.class
)
public class AstraSink implements Sink<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(AstraSink.class);

    private AstraConfig astraConfig;
    
    private List<Record<byte[]>> incomingList;
    private ScheduledExecutorService flushExecutor;

    private String table;
    private volatile String token = null;
    private String baseURL;

    private ScheduledExecutorService tokenRefresher = Executors.newScheduledThreadPool(1);

    private final int TOKEN_REFRESH_INTERVAL_MINUTES = 8;
    
    /**
    * Write a message to Sink
    * @param inputRecordContext Context of input record from the source
    * @param record record to write to sink
    * @throws Exception
    */
    @Override
    public void write(Record<byte[]> record) throws Exception {
        //TODO: toAstra can be its own thread
        toAstra(record);
    }

    @Override
    public void close() throws IOException {
        log.info("astra sink stopped...");
    }

    /**
    * Open connector with configuration
    *
    * @param config initialization config
    * @param sinkContext
    * @throws Exception IO type exceptions when opening a connector
    */
    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("open astra cassandra sink configs size {}", config.size());
        astraConfig = AstraConfig.load(config);

        for (String topicName : sinkContext.getInputTopics()){
            log.info("input topic {}", topicName);
        }

        incomingList = Lists.newArrayList();

        flushExecutor = Executors.newScheduledThreadPool(1);

        if (Strings.isBlank(astraConfig.getClusterId())
            || Strings.isBlank(astraConfig.getClusterRegion())
            || Strings.isBlank(astraConfig.getKeySpace())
            || Strings.isBlank(astraConfig.getTableName())
            || Strings.isBlank(astraConfig.getUsername())
            || Strings.isBlank(astraConfig.getPassword())) {
            
            String err = "clusterId, region, keyspace, table name, and user credentials must be specified.";
            log.error(err);
            throw new IllegalArgumentException(err);
        }
        this.baseURL = "https://" + astraConfig.getClusterId() + "-" + astraConfig.getClusterRegion()
            + ".apps.astra.datastax.com/api/rest/v1/";

        this.table = astraConfig.getTableName();
        this.token = generateToken(); // first generateToken to make sure the token is correct
        String schema = astraConfig.getTableSchema();
        if (Strings.isNotBlank(schema) && createTable(schema)) {
            log.info("successfully created the table {}", table);
        }
        tokenRefresher.scheduleAtFixedRate(() -> {
                try {
                    this.token = generateToken();
                } catch (Exception e) {
                    log.error("failed to generate token by token refresher {}", e);
                }
            
            },
            TOKEN_REFRESH_INTERVAL_MINUTES, TOKEN_REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES
        );
    }

    public static long getLedgerId(long sequenceId) {
        return sequenceId >>> 28;
    }

    private String generateToken() {
        String url = this.baseURL + "auth";
        HttpResponse<JsonNode> response = Unirest.post(url)
        .header("Content-Type", "application/json")
        .header("x-cassandra-request-id", genUUID())
        .body("{\"username\":\"" + astraConfig.getUsername() + "\", \"password\":\"" + astraConfig.getPassword() + "\"}")
        .asJson();

        log.info("code " + response.getStatus());
        if (response.getStatus() != 201) {
            throw new RuntimeException("failed to generate token");
        }

        JSONObject obj = response.getBody().getObject();
        return obj.getString("authToken");
    }

    private boolean createTable(String schema) {
        if (Strings.isBlank(schema)) {
            return true;
        }
        String url = this.baseURL + "keyspaces/" + astraConfig.getKeySpace() + "/tables";

        HttpResponse<String> response = Unirest.post(url)
        .header("x-cassandra-request-id", genUUID())
        .header("x-cassandra-token", this.token)
        .header("Content-Type", "application/json")
        .body(schema)
        .asString();

        if (response.getStatus() != 201) {
            throw new RuntimeException("failed to create table status code " + response.getStatus()
                + " error " + response.getBody());
        }
        return true;
    }

    private void addRow(String rows, String table) {
        log.info("addRow {}", rows);
        String url = this.baseURL +"keyspaces/" + astraConfig.getKeySpace() + "/tables/" + table + "/rows";
        HttpResponse<String> response = Unirest.post(url)
        .header("x-cassandra-request-id", genUUID())
        .header("x-cassandra-token", this.token)
        .header("Content-Type", "application/json")
        .body(rows)
        .asString();

        if (response.getStatus() != 201) {
            throw new RuntimeException("failed to add rows status code " + response.getStatus()
                + " error " + response.getBody());
        }
    }

    private String buildColumnData(Record<byte[]> record) {
        JSONObject obj = new JSONObject(new String(record.getValue()));
        String objStr = "{\"columns\":[";
        for (String key : obj.keySet()) {
          objStr = objStr + "{\"name\":\"" + key + "\",\"value\":\"" + obj.getString(key) + "\"},";
        }

        // remove the last comma
        return StringUtils.substring(objStr, 0, objStr.length() - 1) + "]}";
    }

    private void toAstra(Record<byte[]> record) {
        try {
            addRow(buildColumnData(record), this.table);
        } catch (Exception e) {
            log.error("failed to send to astra ", e);
        }

        record.ack();
    }

    private String genUUID() {
        return UUID.randomUUID().toString();
    }
}