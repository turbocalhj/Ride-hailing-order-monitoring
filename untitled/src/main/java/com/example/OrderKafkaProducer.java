package com.example;

import com.example.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单数据生产者: 从CSV文件读取数据并发送到Kafka
 * 
 * 功能:
 * 1. 读取 NYC TLC 出租车数据 CSV 文件
 * 2. 将每行数据转换为 Order 对象并序列化为 JSON
 * 3. 以 100ms 间隔发送到 Kafka(ride-sharing-orders topic)
 * 
 * 数据清洗:
 * - 过滤价格 < 0 或 > 500 的异常数据
 * - 将 LocationID 映射为 NYC 范围内的经纬度坐标
 */
public class OrderKafkaProducer {

    // ==================== Kafka配置 ====================
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ride-sharing-orders";
    private static final String CSV_FILE_PATH = "yellow_tripdata_2026-01.parquet.csv";

    // ==================== NYC地理坐标范围 ====================
    // 用于将 LocationID 映射为真实的纽约市经纬度
    private static final double NYC_LAT_MIN = 40.5;
    private static final double NYC_LAT_MAX = 40.9;
    private static final double NYC_LNG_MIN = -74.3;
    private static final double NYC_LNG_MAX = -73.7;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicInteger orderCount = new AtomicInteger(0);

        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                System.err.println("CSV文件为空");
                return;
            }
            System.out.println("跳过表头: " + headerLine);

            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] fields = parseCSVLine(line);
                    if (fields.length < 17) {
                        continue;
                    }

                    Order order = new Order();
                    order.setOrderId("order-" + UUID.randomUUID().toString().substring(0, 8));
                    order.setTimestamp(System.currentTimeMillis());

                    int pickupLocationId = parseLocationId(fields[7]);
                    int dropoffLocationId = parseLocationId(fields[8]);

                    order.setStartLat(locationIdToLat(pickupLocationId));
                    order.setStartLng(locationIdToLng(pickupLocationId));
                    order.setEndLat(locationIdToLat(dropoffLocationId));
                    order.setEndLng(locationIdToLng(dropoffLocationId));

                    double price = parsePrice(fields[16]);
                    if (price < 0 || price > 500) {
                        continue;
                    }
                    order.setPrice(price);

                    order.setDriverId("driver-" + (orderCount.get() % 50));
                    order.setPassengerId("passenger-" + (orderCount.get() % 200));

                    String value = objectMapper.writeValueAsString(order);
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, order.getOrderId(), value);
                    producer.send(record);

                    orderCount.incrementAndGet();
                    if (orderCount.get() % 100 == 0) {
                        System.out.println("已发送 " + orderCount.get() + " 条订单");
                    }

                    Thread.sleep(100);  //压测时注释掉

                } catch (Exception e) {
                    // 忽略解析异常的数据行
                }
            }

            System.out.println("数据回放完成，共发送 " + orderCount.get() + " 条订单");

        } catch (Exception e) {
            System.err.println("读取CSV文件失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static String[] parseCSVLine(String line) {
        return line.split(",", -1);
    }

    private static int parseLocationId(String value) {
        try {
            return Integer.parseInt(value.trim().replace("\"", ""));
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private static double parsePrice(String value) {
        try {
            return Double.parseDouble(value.trim().replace("\"", ""));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static double locationIdToLat(int locationId) {
        double normalized = (locationId % 1000) / 1000.0;
        return NYC_LAT_MIN + normalized * (NYC_LAT_MAX - NYC_LAT_MIN);
    }

    private static double locationIdToLng(int locationId) {
        double normalized = ((locationId * 7) % 1000) / 1000.0;
        return NYC_LNG_MIN + normalized * (NYC_LNG_MAX - NYC_LNG_MIN);
    }
}
