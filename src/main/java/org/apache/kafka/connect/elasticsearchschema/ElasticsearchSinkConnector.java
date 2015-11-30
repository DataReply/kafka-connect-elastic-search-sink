package org.apache.kafka.connect.elasticsearchschema;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by a.patelli on 11/19/2015.
 */
public class ElasticsearchSinkConnector extends SinkConnector {
    public static final String CLUSTER_NAME = "elasticsearch.cluster.name";
    public static final String HOSTS = "elasticsearch.hosts";
    public static final String BULK_SIZE = "elasticsearch.bulk.size";
    public static final String INDEX_NAME = "elasticsearch.index.name";
    public static final String DOCUMENT_NAME = "elasticsearch.document.name";
    private String clusterName;
    private String hosts;
    private String bulkSize;
    private String indexName;
    private String documentName;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        clusterName = props.get(CLUSTER_NAME);
        hosts = props.get(HOSTS);
        bulkSize = props.get(BULK_SIZE);
        indexName = props.get(INDEX_NAME);
        documentName = props.get(DOCUMENT_NAME);
        if(clusterName == null || clusterName.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.cluster.name' setting");
        }
        if(hosts == null || hosts.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticserch.hosts' setting");
        }
        if(bulkSize == null || bulkSize.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.bulk.size' setting");
        }
        if(indexName == null || indexName.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.index.name' setting");
        }
        if(documentName == null || documentName.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must incluede 'elasticsearch.document.name' setting");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<String, String>();
            config.put(CLUSTER_NAME, clusterName);
            config.put(HOSTS, hosts);
            config.put(BULK_SIZE, bulkSize);
            config.put(DOCUMENT_NAME, documentName);
            config.put(INDEX_NAME, indexName);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }
}
