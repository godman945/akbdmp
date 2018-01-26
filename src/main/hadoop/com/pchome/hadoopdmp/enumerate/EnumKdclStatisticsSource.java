package com.pchome.hadoopdmp.enumerate;

public enum EnumKdclStatisticsSource {

	MEMID_24_Y("MEMID_24_Y"),
	MEMID_24_N("MEMID_24_N"),
	UUID_24_Y("UUID_24_Y"),
	UUID_24_N("UUID_24_N"),
	MEMID_RUTEN_Y("MEMID_RUTEN_Y"),
	MEMID_RUTEN_N("MEMID_RUTEN_N"),
	UUID_RUTEN_Y("UUID_RUTEN_Y"),
	UUID_RUTEN_N("UUID_RUTEN_N"),
	UUID_ADCLICK_Y("UUID_ADCLICK_Y"),
	MEMID_ADCLICK_Y("MEMID_ADCLICK_Y");
	
	private final String key;

	private EnumKdclStatisticsSource(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}


}
