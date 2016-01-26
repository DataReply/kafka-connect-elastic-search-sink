package org.apache.kafka.connect.elasticsearchschema;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

/**
 * ElasticsearchSinkConnector implement the Connector interface to send Kafka
 * data to Elasticsearch.
 *
 * @author Andrea Patelli
 */
public class ElasticsearchSinkConnector extends SinkConnector {
    public static final String CLUSTER_NAME = "elasticsearch.cluster.name";
    public static final String HOSTS = "elasticsearch.hosts";
    public static final String BULK_SIZE = "elasticsearch.bulk.size";
    public static final String INDEXES= "elasticsearch.indexes";
    public static final String DOCUMENT_NAME = "elasticsearch.document.name";
    public static final String TOPICS= "topics";
    private String clusterName;
    private String hosts;
    private String bulkSize;
    private String documentName;

    public static final Map<String, String> mapping = new HashMap<>(0);

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        clusterName = props.get(CLUSTER_NAME);
        hosts = props.get(HOSTS);
        bulkSize = props.get(BULK_SIZE);
        documentName = props.get(DOCUMENT_NAME);
        if (clusterName == null || clusterName.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.cluster.name' setting");
        }
        if (hosts == null || hosts.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticserch.hosts' setting");
        }
        if (bulkSize == null || bulkSize.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.bulk.size' setting");
        }
        if (documentName == null || documentName.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.document.name' setting");
        }

        String topics = props.get(TOPICS);
        String indexes = props.get(INDEXES);
        if (topics == null || topics.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'topics' setting");
        }
        if (indexes == null || indexes.isEmpty()) {
            throw new ConnectException("ElasticsearchSinkConnector configuration must include 'elasticsearch.document.name' setting");
        }

        List<String> topicsList = Arrays.asList(topics.replaceAll(" ", "").split(","));
        List<String> indexesList = Arrays.asList(topics.replaceAll(" ", "").split(","));

        if(topicsList.size() != indexesList.size()) {
            throw new ConnectException("The number of indexes should be the same as the number of topics");
        }

        for(int i = 0; i < topicsList.size(); i++) {
            mapping.put(topicsList.get(i), indexesList.get(i));
        }
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<String, String>();
            config.put(CLUSTER_NAME, clusterName);
            config.put(HOSTS, hosts);
            config.put(BULK_SIZE, bulkSize);
            config.put(DOCUMENT_NAME, documentName);
            configs.add(config);
        }
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
        // Nothing to do
    }
}
