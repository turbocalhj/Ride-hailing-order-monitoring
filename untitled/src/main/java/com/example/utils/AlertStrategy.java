package com.example.utils;

import com.example.model.AlertLevel;
import com.example.model.Order;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AlertStrategy {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static AlertLevel determineAlertLevel(Order order) {
        double distance = Math.sqrt(
            Math.pow(order.getEndLat() - order.getStartLat(), 2) + 
            Math.pow(order.getEndLng() - order.getStartLng(), 2)
        );

        // 根据异常类型和严重程度设置告警级别
        if (order.getPrice() > 500) {
            return AlertLevel.CRITICAL;
        } else if (order.getPrice() > 200) {
            return AlertLevel.ERROR;
        } else if (distance > 0.5) {
            return AlertLevel.CRITICAL;
        } else if (distance > 0.2) {
            return AlertLevel.ERROR;
        } else {
            // 时间异常
            long currentTime = System.currentTimeMillis();
            if (order.getTimestamp() > currentTime + 3600000 || order.getTimestamp() < currentTime - 86400000) {
                return AlertLevel.WARNING;
            }
        }

        return AlertLevel.INFO;
    }

    public static void executeAlertStrategy(Order order, AlertLevel level) {
        String alertMessage = generateAlertMessage(order, level);
        System.out.println("Alert generated: " + alertMessage);

        // 根据告警级别执行不同的告警策略
        switch (level) {
            case CRITICAL:
                // 严重级别：发送邮件、短信、电话告警
                sendEmailAlert(alertMessage);
                sendSmsAlert(alertMessage);
                break;
            case ERROR:
                // 错误级别：发送邮件、短信告警
                sendEmailAlert(alertMessage);
                sendSmsAlert(alertMessage);
                break;
            case WARNING:
                // 警告级别：发送邮件告警
                sendEmailAlert(alertMessage);
                break;
            case INFO:
                // 信息级别：仅记录日志
                break;
        }
    }

    private static String generateAlertMessage(Order order, AlertLevel level) {
        String timestamp = dateFormat.format(new Date(order.getTimestamp()));
        double distance = Math.sqrt(
            Math.pow(order.getEndLat() - order.getStartLat(), 2) + 
            Math.pow(order.getEndLng() - order.getStartLng(), 2)
        );

        String anomalyType;
        if (order.getPrice() > 200) {
            anomalyType = "Price Anomaly";
        } else if (distance > 0.2) {
            anomalyType = "Distance Anomaly";
        } else {
            anomalyType = "Time Anomaly";
        }

        return String.format("[%s] [%s] %s: Order ID: %s, Price: %.2f, Distance: %.4f, Driver: %s, Passenger: %s",
                dateFormat.format(new Date()),
                level.getCode(),
                anomalyType,
                order.getOrderId(),
                order.getPrice(),
                distance,
                order.getDriverId(),
                order.getPassengerId());
    }

    private static void sendEmailAlert(String message) {
        try {
            // 添加频率控制：每30秒最多发送一次邮件
            Thread.sleep(5000);
            EmailUtils.sendEmail("网约车订单异常警告", message);
        } catch (Exception e) {
            System.err.println("Failed to send email alert: " + e.getMessage());
        }
    }

    private static void sendSmsAlert(String message) {
        // 模拟发送短信告警
        System.out.println("Sending SMS alert: " + message);
        // 实际项目中可以集成短信API
    }
}