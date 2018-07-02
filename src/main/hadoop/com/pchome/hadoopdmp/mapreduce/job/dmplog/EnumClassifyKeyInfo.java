package com.pchome.hadoopdmp.mapreduce.job.dmplog;

public enum EnumClassifyKeyInfo {

	memid_kdcl_log_personal_info_api_Y("1","分類資訊"),
	all_kdcl_log_personal_info_Y("2","性別資訊"),
	all_kdcl_log_class_ad_click_Y("3","年齡資訊"),
	all_kdcl_log_class_24h_url_Y("4","國家資訊"),
	all_kdcl_log_class_ruten_url_Y("5","城市資訊"),
	all_kdcl_log_area_info_Y("6","裝置資訊"),
	all_kdcl_log_device_info_Y("7","手機資訊"),
	all_kdcl_log_time_info_Y("8","瀏覽器資訊"),
	
	memid_kdcl_log_personal_info_api_N("1","分類資訊"),
	all_kdcl_log_personal_info_N("2","性別資訊"),
	all_kdcl_log_class_ad_click_N("3","年齡資訊"),
	all_kdcl_log_class_24h_url_N("4","國家資訊"),
	all_kdcl_log_class_ruten_url_N("5","城市資訊"),
	all_kdcl_log_area_info_N("6","裝置資訊"),
	all_kdcl_log_device_info_N("7","手機資訊"),
	all_kdcl_log_time_info_N("8","瀏覽器資訊"),
	
	
	
	memid_camp_log_personal_info_api_Y("9","作業系統資訊"),
	all_camp_log_personal_info_Y("10","時間資訊"),
	all_camp_log_class_ad_click_Y("10","時間資訊"),
	all_camp_log_class_24h_url_Y("10","時間資訊"),
	all_camp_log_class_ruten_url_Y("10","時間資訊"),
	all_camp_log_area_info_Y("10","時間資訊"),
	all_camp_log_device_info_Y("10","時間資訊"),
	all_camp_log_time_info_Y("10","時間資訊"),
	
	memid_camp_log_personal_info_api_N("9","作業系統資訊"),
	all_camp_log_personal_info_N("10","時間資訊"),
	all_camp_log_class_ad_click_N("10","時間資訊"),
	all_camp_log_class_24h_url_N("10","時間資訊"),
	all_camp_log_class_ruten_url_N("10","時間資訊"),
	all_camp_log_area_info_N("10","時間資訊"),
	all_camp_log_device_info_N("10","時間資訊"),
	all_camp_log_time_info_N("10","時間資訊");
	
	private final String type;
	private final String chName;
	
	private EnumClassifyKeyInfo(String type, String chName) {
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
