package com.pchome.dmp.enumerate;


public enum EnumKdclPvDailyAddedAmount {

	Categorized_pcid_ruten(
			"pv_ruten_cated_pcid", "memid", "kdcl", "ruten", "Y"
			),
	Categorized_uuid_ruten(
			"pv_ruten_cated_uuid", "uuid", "kdcl", "ruten", "Y"
			),
	Uncategorized_pcid_ruten(
			"pv_ruten_uncated_pcid", "memid", "kdcl", "ruten", "N"
			),
	Uncategorized_uuid_ruten(
			"pv_ruten_uncated_uuid", "uuid", "kdcl", "ruten", "N"
			),

	Categorized_pcid_24h(
			"pv_24h_cated_pcid", "memid", "kdcl", "24h", "Y"
			),
	Categorized_uuid_24h(
			"pv_24h_cated_uuid", "uuid", "kdcl", "24h", "Y"
			),
	Uncategorized_pcid_24h(
			"pv_24h_uncated_pcid", "memid", "kdcl", "24h", "N"
			),
	Uncategorized_uuid_24h(
			"pv_24h_uncated_uuid", "uuid", "kdcl", "24h", "N"
			),;

	private String keyName;
	private String id_type;
	private String service_type;
	private String behavior;
	private String classify;

	private EnumKdclPvDailyAddedAmount(String keyName, String id_type, String service_type, String behavior, String classify) {
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
