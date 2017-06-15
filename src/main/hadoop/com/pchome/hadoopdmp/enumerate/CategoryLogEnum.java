package com.pchome.hadoopdmp.enumerate;

public enum CategoryLogEnum {

	AD_CLICK("AD_CLICK", "user點擊ad_class"),
	PV_RETUN("PV_RETUN", "PV_RETUN"),
	PV_24H("PV_24H", "PV_24H");
	
	private final String key;
	private final String value;

	private CategoryLogEnum(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

}
