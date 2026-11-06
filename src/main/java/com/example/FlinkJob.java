package com.example;

import com.example.model.Order;
import com.example.sinks.AlertSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 基于Flink与Kafka的网约车实时订单流监控与异常警告系统
 * 
 * 系统架构:
 * Kafka(ride-sharing-orders) → Flink异常检测 → Kafka(processed-orders / anomaly-orders)
 *                                              → ElasticsearchConsumer → Elasticsearch
 *                                              → AlertSink → 控制台告警
 * 
 * 异常检测基于NYC TLC官方计费标准:
 * - 起步价: $3.00
 * - 每公里费率: $2.17/km
 * - 附加费容忍度: $25.0 (覆盖等候费、夜间附加费、过桥费等)
 */
public class FlinkJob {

    // ==================== Kafka配置 ====================
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "ride-sharing-orders";       // 原始订单数据输入
    private static final String PROCESSED_TOPIC = "processed-orders";       // 正常订单输出
    private static final String ANOMALY_TOPIC = "anomaly-orders";           // 异常订单输出
    private static final String CONSUMER_GROUP = "flink-ride-sharing-group";

    // ==================== Flink侧输出流标签 ====================
    // 用于将异常订单从主流分离到侧输出流
    private static final OutputTag<Order> ANOMALY_TAG = new OutputTag<Order>("anomaly-orders") {};

    // ==================== 异常检测阈值 (基于NYC TLC标准) ====================
    // 距离异常阈值: 网约车为城市内短途出行, 超过50km判定为GPS漂移或异常长途订单
    // NYC最大对角线距离约65km, 50km可捕获跨城区的异常长距离订单
    private static final double DISTANCE_ANOMALY_THRESHOLD_KM = 50.0;

    // TLC计费模型参数
    private static final double TLC_BASE_FARE = 3.00;           // 起步基准价 ($3.00)
    private static final double TLC_PRICE_PER_KM = 2.17;        // 单位里程费率 ($2.17/km)
    private static final double TLC_TOLERANCE = 25.0;           // 交通拥堵与附加费容忍度 ($25.0)
    private static final double SHORT_TRIP_DISTANCE_KM = 0.1;   // 短途距离阈值 (0.1km)
    private static final double SHORT_TRIP_MAX_PRICE = 15.0;    // 短途最高价格阈值 ($15.0)

    public static void main(String[] args) throws Exception {
        // 创建Flink流处理执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //测试模块（强制开启本地 8081 端口的 Web UI）
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 设置并行度为4, 充分利用多核CPU
        env.setParallelism(4);

        // 开启Checkpoint机制, 保证Exactly-Once语义
        env.enableCheckpointing(10000);  // 每10秒执行一次Checkpoint
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);   // 两次Checkpoint最小间隔5秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);           // Checkpoint超时时间60秒
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);        // 最多同时执行1个Checkpoint

        // ==================== 数据源: 从Kafka读取原始订单数据 ====================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())  // 从最早的消息开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // JSON反序列化: 将Kafka中的JSON字符串转换为Order对象
        ObjectMapper objectMapper = new ObjectMapper();
        DataStream<Order> orderStream = sourceStream.map(json -> objectMapper.readValue(json, Order.class))
                .returns(Order.class);

        // 设置Watermark策略: 允许3秒的乱序容忍度
        WatermarkStrategy<Order> watermarkStrategy = WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((order, recordTimestamp) -> order.getTimestamp());

        // ==================== 核心处理: 异常检测 ====================
        // 使用ProcessFunction进行逐条订单的异常检测
        // 正常订单 → 主流 (processedStream)
        // 异常订单 → 侧输出流 (ANOMALY_TAG)
        SingleOutputStreamOperator<Order> processedStream = orderStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .process(new OrderProcessFunction());

        // 提取侧输出流中的异常订单
        DataStream<Order> anomalyStream = processedStream.getSideOutput(ANOMALY_TAG);

        // ==================== 窗口聚合: 每10秒计算一次总营收 ====================
        DataStream<Order> aggregatedStream = processedStream
                .keyBy(order -> "ALL")  // 将所有订单分到同一个key
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 10秒滚动窗口
                .reduce(new OrderPriceSumReduceFunction());

        // ==================== 数据输出: 正常订单写入Kafka ====================
        KafkaSink<String> processedSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(PROCESSED_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // 正常订单流 → Kafka(processed-orders) → Elasticsearch(metrics索引)
        processedStream.map(order -> objectMapper.writeValueAsString(order))
                .returns(Types.STRING)
                .sinkTo(processedSink);

        // 聚合数据流 → Kafka(processed-orders) → Elasticsearch(metrics索引)
        aggregatedStream.map(order -> objectMapper.writeValueAsString(order))
                .returns(Types.STRING)
                .sinkTo(processedSink);

        // ==================== 数据输出: 异常订单写入Kafka ====================
        KafkaSink<String> anomalySink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ANOMALY_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // 异常订单流 → Kafka(anomaly-orders) → Elasticsearch(alerts索引)
        anomalyStream.map(order -> objectMapper.writeValueAsString(order))
                .returns(Types.STRING)
                .sinkTo(anomalySink);

        // 异常订单同时发送到AlertSink进行控制台告警
        anomalyStream.addSink(new AlertSink()).setParallelism(1);

        // 启动Flink作业
        env.execute("Ride Sharing Real-time Order Monitor");
    }

    private static class OrderProcessFunction extends ProcessFunction<Order, Order> {
        @Override
        public void processElement(Order order, Context ctx, Collector<Order> out) throws Exception {
            double distanceKm = calculateHaversineDistance(
                    order.getStartLat(), order.getStartLng(),
                    order.getEndLat(), order.getEndLng());

            boolean isDistanceAnomaly = distanceKm > DISTANCE_ANOMALY_THRESHOLD_KM;

            boolean isPriceAnomaly = false;
            String anomalyReason = "";

            if (order.getPrice() <= 0) {
                isPriceAnomaly = true;
                anomalyReason = "价格异常: 价格<=0, 实际价格=" + order.getPrice();
            } else if (distanceKm < SHORT_TRIP_DISTANCE_KM && order.getPrice() > SHORT_TRIP_MAX_PRICE) {
                isPriceAnomaly = true;
                anomalyReason = String.format("价格异常: 短途高价, 距离=%.2fkm, 实际价格=%.2f, 阈值=%.2f",
                        distanceKm, order.getPrice(), SHORT_TRIP_MAX_PRICE);
            } else {
                double expectedMaxPrice = TLC_BASE_FARE + (distanceKm * TLC_PRICE_PER_KM) + TLC_TOLERANCE;
                if (order.getPrice() > expectedMaxPrice) {
                    isPriceAnomaly = true;
                    anomalyReason = String.format("价格异常: 超出TLC监管上限, 实际价格=%.2f, 理论上限=%.2f, 距离=%.2fkm",
                            order.getPrice(), expectedMaxPrice, distanceKm);
                }
            }

            if (isDistanceAnomaly || isPriceAnomaly) {
                if (isDistanceAnomaly && isPriceAnomaly) {
                    order.setAnomalyType("距离异常+价格异常");
                } else if (isDistanceAnomaly) {
                    order.setAnomalyType("距离异常");
                } else {
                    order.setAnomalyType("价格异常");
                }

                if (isDistanceAnomaly) {
                    System.out.println(String.format("距离异常: orderId=%s, 距离=%.2fkm, 阈值=%.2fkm",
                            order.getOrderId(), distanceKm, DISTANCE_ANOMALY_THRESHOLD_KM));
                }
                if (isPriceAnomaly) {
                    System.out.println(anomalyReason);
                }
                ctx.output(ANOMALY_TAG, order);
            } else {
                out.collect(order);
            }
        }
    }

    private static class OrderPriceSumReduceFunction implements ReduceFunction<Order> {
        @Override
        public Order reduce(Order value1, Order value2) throws Exception {
            Order result = new Order();
            result.setOrderId("aggregated-" + System.currentTimeMillis());
            result.setTimestamp(Math.max(value1.getTimestamp(), value2.getTimestamp()));
            result.setPrice(value1.getPrice() + value2.getPrice());
            result.setDriverId("AGGREGATED");
            result.setPassengerId("WINDOW_SUM");
            return result;
        }
    }

    private static double calculateHaversineDistance(double lat1, double lon1, double lat2, double lon2) {
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
}
