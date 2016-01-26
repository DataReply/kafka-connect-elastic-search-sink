package org.apache.kafka.connect.elasticsearchschema;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;

/**
 * ElasticsearchSinkTask is a Task that takes records loaded from Kafka and sends them to
 * another system.
 *
 * @author Andrea Patelli
 */
public class ElasticsearchSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkConnector.class);

    private String clusterName;
    private String hosts;
    private Integer bulkSize;
    private String documentName;
    Client client;


    public ElasticsearchSinkTask() {
    }

    public String version() {
        return new ElasticsearchSinkConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        clusterName = props.get(ElasticsearchSinkConnector.CLUSTER_NAME);
        hosts = props.get(ElasticsearchSinkConnector.HOSTS);
        documentName = props.get(ElasticsearchSinkConnector.DOCUMENT_NAME);
        try {
            bulkSize = Integer.parseInt(props.get(ElasticsearchSinkConnector.BULK_SIZE));
        } catch (Exception e) {
            throw new ConnectException("Setting elasticsearch.bulk.size should be an integer");
        }
        List<String> hostsList = new ArrayList<>(Arrays.asList(hosts.replaceAll(" ", "").split(",")));

        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName).build();

            client = TransportClient.builder().settings(settings).build();

            for (String host : hostsList) {
                String address;
                Integer port;
                String[] hostArray = host.split(":");
                address = hostArray[0];
                try {
                    port = Integer.parseInt(hostArray[1]);
                } catch (Exception e) {
                    port = 9300;
                }
                ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), port));
            }
        } catch (Exception e) {
            throw new ConnectException("Impossible to connect to hosts");
        }

    }

    /**
     * Put the records in the sink.
     *
     * @param sinkRecords the set of records to send.
     */
    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
//        for(SinkRecord record : sinkRecords) {
//            //convert the record in json and write it to elasticsearch
//            Map<String, Object> jsonMap = toJsonMap((Struct)record.value());
//
//            client.prepareIndex(record.topic(), record.topic())
//                    .setSource(jsonMap)
//                    .get();
//        }
        List<SinkRecord> records = new ArrayList<SinkRecord>(sinkRecords);
        for (int i = 0; i < records.size(); i++) {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (int j = 0; j < bulkSize && i < records.size(); j++, i++) {
                SinkRecord record = records.get(i);
                Map<String, Object> jsonMap = toJsonMap((Struct) record.value());
                jsonMap.put("_id", record.kafkaOffset());
                bulkRequest.add(
                        client
                                .prepareIndex(
                                        ElasticsearchSinkConnector.mapping.get(record.topic()),
                                        documentName
                                )
                                .setSource(jsonMap)
                );
            }
            i--;
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse item : bulkResponse) {
                    log.error(item.getFailureMessage());
                }
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        //don't know if needed
    }

    @Override
    public void stop() {
        //close connection
        client.close();
    }

    private Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case INT32:
                    jsonMap.put(fieldName, struct.getInt32(fieldName));
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT64:
                    jsonMap.put(fieldName, struct.getInt64(fieldName));
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }
}