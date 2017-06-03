package com.pchome.akbdmp.api.data.enumeration;

public enum DmpLifeCheckEnum {

	CHECK_KEY("LifeCheck", "I am fine");
	
	private final String key;
	private final String value;

	private DmpLifeCheckEnum(String key, String value) {
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
