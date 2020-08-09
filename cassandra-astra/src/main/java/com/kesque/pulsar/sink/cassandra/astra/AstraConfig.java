package com.kesque.pulsar.sink.cassandra.astra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.Serializable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AstraConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String username = "";
    public String getUsername() {
        return this.username;
    }
    public void setUsername(String username) {
        this.username = username;
    }

    private String password = "";
    public String getPassword() {
        return this.password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    private String keySpace = "";
    public String getKeySpace() {
        return this.keySpace;
    }
    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    private String clusterRegion = "us-east1";
    public String getClusterRegion() {
        return this.clusterRegion;
    }
    public void setClusterRegion(String clusterRegion) {
        this.clusterRegion = clusterRegion;
    }

    private String clusterId = "";
    public String getClusterId() {
        return this.clusterId;
    }
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    private String tableName = "";
    public String getTableName() {
        return this.tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private String tableSchema = "";
    public String getTableSchema() {
        return this.tableSchema;
    }
    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
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

    public static AstraConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), AstraConfig.class);
    }

    public static AstraConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), AstraConfig.class);
    }
    
}