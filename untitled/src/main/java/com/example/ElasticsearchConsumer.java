package com.example;

import com.example.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Elasticsearch消费者: 从Kafka消费订单数据并写入Elasticsearch
 * 
 * 功能:
 * 1. 订阅 processed-orders 和 anomaly-orders 两个Kafka主题
 * 2. 正常订单 → ride-sharing-metrics-v2 索引 (用于Grafana指标展示)
 * 3. 异常订单 → ride-sharing-alerts-v2 索引 (用于Grafana告警展示)
 * 4. 使用Bulk API批量写入, 提高写入效率
 * 
 * 注意:
 * - anomalyType 字段由 FlinkJob 的 OrderProcessFunction 设置, 此处直接使用
 * - 距离和每公里单价在此处重新计算, 用于Grafana面板展示
 */
public class ElasticsearchConsumer {

    // ==================== Kafka配置 ====================
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String[] TOPICS = {"processed-orders", "anomaly-orders"};
    private static final String GROUP_ID = "elasticsearch-consumer-group";

    // ==================== Elasticsearch配置 ====================
    private static final String ELASTICSEARCH_HOST = "localhost";
    private static final int ELASTICSEARCH_PORT = 9200;

    // ==================== Elasticsearch索引名称 ====================
    private static final String METRICS_INDEX = "ride-sharing-metrics-v2";  // 正常订单指标数据
    private static final String ALERTS_INDEX = "ride-sharing-alerts-v2";    // 异常订单告警数据
    private static final String PROCESSED_TOPIC = "processed-orders";
    private static final String ANOMALY_TOPIC = "anomaly-orders";

    private RestHighLevelClient esClient;
    private KafkaConsumer<String, String> kafkaConsumer;
    private ObjectMapper objectMapper;

    public ElasticsearchConsumer() {
        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, "http"))
        );

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList(TOPICS));

        objectMapper = new ObjectMapper();
    }

    public void consume() {
        System.out.println("Elasticsearch消费者启动，订阅主题: " + Arrays.toString(TOPICS));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    BulkRequest metricsBulkRequest = new BulkRequest();
                    BulkRequest alertsBulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            String topic = record.topic();
                            String value = record.value();

                            if (ANOMALY_TOPIC.equals(topic)) {
                                Order order = objectMapper.readValue(value, Order.class);
                                double distance = calculateHaversineDistance(
                                        order.getStartLat(), order.getStartLng(),
                                        order.getEndLat(), order.getEndLng());
                                double pricePerKm = (distance > 0) ? (order.getPrice() / distance) : 0.0;

                                String anomalyType = order.getAnomalyType();
                                if (anomalyType == null || anomalyType.isEmpty()) {
                                    anomalyType = "未知异常";
                                }

                                ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(value);
                                jsonNode.put("anomalyType", anomalyType);
                                jsonNode.put("distance", distance);
                                jsonNode.put("pricePerKm", pricePerKm);

                                IndexRequest indexRequest = new IndexRequest(ALERTS_INDEX)
                                        .id(generateDocumentId(record))
                                        .source(jsonNode.toString(), XContentType.JSON);
                                alertsBulkRequest.add(indexRequest);
                            } else if (PROCESSED_TOPIC.equals(topic)) {
                                IndexRequest indexRequest = new IndexRequest(METRICS_INDEX)
                                        .id(generateDocumentId(record))
                                        .source(value, XContentType.JSON);
                                metricsBulkRequest.add(indexRequest);
                            }

                        } catch (Exception e) {
                            System.err.println("解析消息失败: " + e.getMessage());
                        }
                    }

                    if (metricsBulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = esClient.bulk(metricsBulkRequest, RequestOptions.DEFAULT);
                        if (bulkResponse.hasFailures()) {
                            System.err.println("指标数据批量写入失败: " + bulkResponse.buildFailureMessage());
                        } else {
                            System.out.println("成功写入 " + metricsBulkRequest.numberOfActions() + " 条指标数据到 " + METRICS_INDEX);
                        }
                    }

                    if (alertsBulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = esClient.bulk(alertsBulkRequest, RequestOptions.DEFAULT);
                        if (bulkResponse.hasFailures()) {
                            System.err.println("告警数据批量写入失败: " + bulkResponse.buildFailureMessage());
                        } else {
                            System.out.println("成功写入 " + alertsBulkRequest.numberOfActions() + " 条告警数据到 " + ALERTS_INDEX);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("消费过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            close();
        }
    }

    private String generateDocumentId(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    private double calculateHaversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final double EARTH_RADIUS_KM = 6371.0;
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLatRad = Math.toRadians(lat2 - lat1);
        double deltaLonRad = Math.toRadians(lon2 - lon1);
        double a = Math.sin(deltaLatRad / 2) * Math.sin(deltaLatRad / 2)
                + Math.cos(lat1Rad) * Math.cos(lat2Rad)
                * Math.sin(deltaLonRad / 2) * Math.sin(deltaLonRad / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_KM * c;
    }

    public void close() {
        try {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
            if (esClient != null) {
                esClient.close();
            }
        } catch (Exception e) {
            System.err.println("关闭资源时发生错误: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        ElasticsearchConsumer consumer = new ElasticsearchConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("正在关闭Elasticsearch消费者...");
            consumer.close();
        }));

        consumer.consume();
    }
}
