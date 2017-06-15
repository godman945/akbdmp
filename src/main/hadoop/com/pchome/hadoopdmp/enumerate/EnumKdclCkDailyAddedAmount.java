package com.pchome.hadoopdmp.enumerate;


public enum EnumKdclCkDailyAddedAmount {

	Categorized_pcid(
			"ck_cated_pcid", "memid", "kdcl", "ad_click", "Y"
			),
	Categorized_uuid(
			"ck_cated_uuid", "uuid", "kdcl", "ad_click", "Y"
			);

	private String keyName;
	private String id_type;
	private String service_type;
	private String behavior;
	private String classify;

	private EnumKdclCkDailyAddedAmount(String keyName, String id_type, String service_type, String behavior, String classify) {
		this.keyName = keyName;
		this.id_type = id_type;
		this.service_type = service_type;
		this.behavior = behavior;
		this.classify = classify;
	}

	public String getKeyName() {
		return keyName;
	}

	public String getId_type() {
		return id_type;
	}

	public String getService_type() {
		return service_type;
	}

	public String getBehavior() {
		return behavior;
	}

	public String getClassify() {
		return classify;
	}



}
