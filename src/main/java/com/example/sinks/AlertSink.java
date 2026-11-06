package com.example.sinks;

import com.example.model.Order;
import com.example.utils.EmailUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 告警输出Sink: 将异常订单打印到控制台并发送邮件告警
 * 
 * 功能:
 * 1. 接收来自Flink侧输出流的异常订单
 * 2. 根据anomalyType设置告警级别 (CRITICAL/ERROR/WARNING)
 * 3. 打印告警信息到控制台
 * 4. CRITICAL和ERROR级别触发邮件告警 (30秒限流)
 * 
 * 告警级别:
 * - CRITICAL: 同时触发价格异常和距离异常
 * - ERROR: 仅触发价格异常
 * - WARNING: 仅触发距离异常
 */
public class AlertSink implements SinkFunction<Order> {

    // 邮件发送限流: 每30秒最多发送一封告警邮件
    private static final long EMAIL_INTERVAL_MS = 30000;
    private static final AtomicLong lastEmailTime = new AtomicLong(0);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void invoke(Order order, Context context) throws Exception {
        double distance = calculateHaversineDistance(
                order.getStartLat(), order.getStartLng(),
                order.getEndLat(), order.getEndLng());

        double pricePerKm = (distance > 0) ? (order.getPrice() / distance) : 0.0;

        String anomalyType = order.getAnomalyType();
        if (anomalyType == null || anomalyType.isEmpty()) {
            anomalyType = "未知异常";
        }

        String alertLevel;
        if (anomalyType.contains("价格异常") && anomalyType.contains("距离异常")) {
            alertLevel = "CRITICAL";
        } else if (anomalyType.contains("价格异常")) {
            alertLevel = "ERROR";
        } else {
            alertLevel = "WARNING";
        }

        String alertMessage = String.format(
                "[%s] [%s] %s | 订单ID: %s | 价格: %.2f元 | 距离: %.2fkm | 每公里单价: %.2f元/km | 司机: %s | 乘客: %s",
                dateFormat.format(new Date()),
                alertLevel,
                anomalyType,
                order.getOrderId(),
                order.getPrice(),
                distance,
                pricePerKm,
                order.getDriverId(),
                order.getPassengerId());

        System.out.println("[ALERT] " + alertMessage);

        if (alertLevel.equals("CRITICAL") || alertLevel.equals("ERROR")) {
            sendEmailWithRateLimit(alertMessage);
        }
    }

    private void sendEmailWithRateLimit(String message) {
        long now = System.currentTimeMillis();
        long lastTime = lastEmailTime.get();

        if (now - lastTime >= EMAIL_INTERVAL_MS) {
            if (lastEmailTime.compareAndSet(lastTime, now)) {
                try {
                    EmailUtils.sendEmail("网约车订单异常告警", message);
                    System.out.println("[EMAIL] 告警邮件发送成功");
                } catch (Exception e) {
                    System.err.println("[EMAIL] 告警邮件发送失败: " + e.getMessage());
                }
            }
        }
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
}