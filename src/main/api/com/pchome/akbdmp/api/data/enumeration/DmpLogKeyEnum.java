package com.pchome.akbdmp.api.data.enumeration;

public enum DmpLogKeyEnum {
	KDCL_MEMID_API_Y("memid_kdcl_log_personal_info_api_Y","kdcl_log","personal_info_api","Y","memid"),
	KDCL_MEMID_API_N("memid_kdcl_log_personal_info_api_N","kdcl_log","personal_info_api","N","memid"), 
	KDCL_PERSONAL_Y("all_kdcl_log_personal_info_Y","kdcl_log","personal_info","Y","all"),
	KDCL_PERSONAL_N("all_kdcl_log_personal_info_N","kdcl_log","personal_info","N","all"),
	KDCL_CLICK_Y("all_kdcl_log_class_ad_click_Y","kdcl_log","class_ad_click","Y","all"),
	KDCL_CLICK_N("all_kdcl_log_class_ad_click_N","kdcl_log","class_ad_click","N","all"),
	KDCL_24_Y("all_kdcl_log_class_24h_url_Y","kdcl_log","class_24h_url","Y","all"),
	KDCL_24_N("all_kdcl_log_class_24h_url_N","kdcl_log","class_24h_url","N","all"),
	KDCL_RUTEN_Y("all_kdcl_log_class_ruten_url_Y","kdcl_log","class_ruten_url","Y","all"),
	KDCL_RUTEN_N("all_kdcl_log_class_ruten_url_N","kdcl_log","class_ruten_url","N","all"),
	KDCL_AREA_Y("all_kdcl_log_area_info_Y","kdcl_log","area_info","Y","all"),
	KDCL_AREA_N("all_kdcl_log_area_info_N","kdcl_log","area_info","N","all"),
	KDCL_DEVICE_Y("all_kdcl_log_device_info_Y","kdcl_log","device_info","Y","all"),
	KDCL_DEVICE_N("all_kdcl_log_device_info_N","kdcl_log","device_info","N","all"),
	KDCL_TIME_Y("all_kdcl_log_time_info_Y","kdcl_log","time_info","Y","all"),
	KDCL_TIME_N("all_kdcl_log_time_info_N","kdcl_log","time_info","N","all"),
	CAMP_MEMID_API_Y("memid_camp_log_personal_info_api_Y","camp_log","personal_info_api","Y","memid"),
	CAMP_MEMID_API_N("memid_camp_log_personal_info_api_N","camp_log","personal_info_api","N","memid"),
	CAMP_PERSONAL_Y("all_camp_log_personal_info_Y","camp_log","personal_info","Y","all"),
	CAMP_PERSONAL_N("all_camp_log_personal_info_N","camp_log","personal_info","N","all"),
	CAMP_CLICK_Y("all_camp_log_class_ad_click_Y","camp_log","class_ad_click","Y","all"),
	CAMP_CLICK_N("all_camp_log_class_ad_click_N","camp_log","class_ad_click","N","all"),
	CAMP_AREA_Y("all_camp_log_area_info_Y","camp_log","area_info","Y","all"),
	CAMP_AREA_N("all_camp_log_area_info_N","camp_log","area_info","N","all"),
	CAMP_DEVICE_Y("all_camp_log_device_info_Y","camp_log","device_info","Y","all"),
	CAMP_DEVICE_N("all_camp_log_device_info_N","camp_log","device_info","N","all"),
	CAMP_TIME_Y("all_camp_log_time_info_Y","camp_log","time_info","Y","all"),
	CAMP_TIME_N("all_camp_log_time_info_N","camp_log","time_info","N","all");
	
	private final String key;
	private final String serviceType;
	private final String behavior;
	private final String classify;
	private final String idType;

	private DmpLogKeyEnum(String key,String type,String behavior,String classify,String idType) {
		this.key = key;
		this.serviceType = type;
		this.behavior = behavior;
		this.classify = classify;
		this.idType = idType;
	}


	public String getBehavior() {
		return behavior;
	}

	public String getClassify() {
		return classify;
	}

	public String getKey() {
		return key;
	}

	public String getServiceType() {
		return serviceType;
	}

	public String getIdType() {
		return idType;
	}

}