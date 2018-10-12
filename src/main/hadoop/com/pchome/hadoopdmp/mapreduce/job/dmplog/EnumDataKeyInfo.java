package com.pchome.hadoopdmp.mapreduce.job.dmplog;

public enum EnumDataKeyInfo {

	category_info("1","分類資訊"),
	sex_info("2","性別資訊"),
	age_info("3","年齡資訊"),
	area_country_info("4","國家資訊"),
	area_city_info("5","城市資訊"),
	device_info("6","裝置資訊"),
	device_phone_info("7","手機資訊"),
	device_browser_info("8","瀏覽器資訊"),
	device_os_info("9","作業系統資訊"),
	time_info("10","時間資訊");
	
	private final String type;
	private final String chName;
	
	private EnumDataKeyInfo(String type, String chName) {
		this.type = type;
		this.chName = chName;
	}
	
	public String getType() {
		return type;
	}
	
	public String getChName() {
		return chName;
	}
	
}
