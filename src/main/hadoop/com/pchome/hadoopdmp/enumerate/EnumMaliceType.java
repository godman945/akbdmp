package com.pchome.hadoopdmp.enumerate;

public enum EnumMaliceType {
    OK("0"),
    UUID("1"),
    REMOTE_IP("2"),
    REFERER("3"),
    USER_AGENT("4"),
    OVER_HOUR_LIMIT("5");

    private String type;

    private EnumMaliceType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}