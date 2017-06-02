package com.pchome.akbdmp.api.data.enumeration;

public enum DmpAdShowLimitParamaterEnum {

	AD_KEY("adKey", "廣告頻次key");
	
	private final String key;
	private final String value;

	private DmpAdShowLimitParamaterEnum(String key, String value) {
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
