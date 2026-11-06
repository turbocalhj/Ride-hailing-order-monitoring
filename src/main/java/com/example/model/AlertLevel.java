package com.example.model;

public enum AlertLevel {
    INFO("INFO", "信息"),
    WARNING("WARNING", "警告"),
    ERROR("ERROR", "错误"),
    CRITICAL("CRITICAL", "严重");

    private final String code;
    private final String description;

    AlertLevel(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}