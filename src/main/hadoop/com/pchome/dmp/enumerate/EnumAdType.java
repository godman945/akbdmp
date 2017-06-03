package com.pchome.dmp.enumerate;

public enum EnumAdType {
    keyword("1"),
    display("2");

    private String adType;

    private EnumAdType(String adType) {
        this.adType = adType;
    }

    public String getAdType() {
        return adType;
    }
}