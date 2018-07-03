package com.pchome.hadoopdmp.mapreduce.job.dmplog;

public enum EnumClassifyKeyInfo {
	//kdcl classify key
	memid_kdcl_log_personal_info_api_Y("1","kdcl個資API有分類"),
	memid_kdcl_log_personal_info_api_N("2","kdcl個資API無分類"),
	all_kdcl_log_personal_info_Y("3","kdcl個資有分類"),
	all_kdcl_log_personal_info_N("4","kdcl個資無分類"),
	all_kdcl_log_class_ad_click_Y("5","kdcl adclick有分類"),
	all_kdcl_log_class_ad_click_N("6","kdcl adclick無分類"),
	all_kdcl_log_class_24h_url_Y("7","kdcl 24h url有分類"),
	all_kdcl_log_class_24h_url_N("8","kdcl 24h url無分類"),
	all_kdcl_log_class_ruten_url_Y("9","kdcl ruten url有分類"),
	all_kdcl_log_class_ruten_url_N("10","kdcl ruten url無分類"),
	all_kdcl_log_area_info_Y("11","kdcl國家、城市有分類"),
	all_kdcl_log_area_info_N("12","kdcl國家、城市無分類"),
	all_kdcl_log_device_info_Y("13","kdcl設備有分類"),
	all_kdcl_log_device_info_N("14","kdcl設備無分類"),
	all_kdcl_log_time_info_Y("15","kdcl時間有分類"),
	all_kdcl_log_time_info_N("16","kdcl時間無分類"),
	
	//campaign classify key
	memid_camp_log_personal_info_api_Y("1","campaign個資API有分類"),
	memid_camp_log_personal_info_api_N("2","campaign個資API無分類"),
	all_camp_log_personal_info_Y("3","campaign個資有分類"),
	all_camp_log_personal_info_N("4","campaign個資無分類"),
	all_camp_log_class_ad_click_Y("5","campaign adclick有分類"),
	all_camp_log_class_ad_click_N("6","campaign adclick無分類"),
	all_camp_log_class_24h_url_Y("7","campaign 24h url有分類"),
	all_camp_log_class_24h_url_N("8","campaign 24h url無分類"),
	all_camp_log_class_ruten_url_Y("9","campaign ruten url有分類"),
	all_camp_log_class_ruten_url_N("10","campaign ruten url無分類"),
	all_camp_log_area_info_Y("11","campaign國家、城市有分類"),
	all_camp_log_area_info_N("12","campaign國家、城市無分類"),
	all_camp_log_device_info_Y("13","campaign設備有分類"),
	all_camp_log_device_info_N("14","campaign設備無分類"),
	all_camp_log_time_info_Y("15","campaign時間有分類"),
	all_camp_log_time_info_N("16","campaign時間無分類");
	
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
