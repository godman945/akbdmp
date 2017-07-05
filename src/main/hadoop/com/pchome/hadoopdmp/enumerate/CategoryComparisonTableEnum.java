package com.pchome.hadoopdmp.enumerate;

public enum CategoryComparisonTableEnum {

	type000001("2671267226750000", "口紅"),
	type000002("0004008900000000", "衣服"),
	type000003("0015022520180000", "帽子"),
	type000004("0000000000000001", "媽媽族"),
	type000005("0000000000000002", "熟女族"),
	type000006("M", "男"),
	type000007("F", "女"),
	type000008("age01to10", "年齡 01 ~ 10"),
	type000009("age11to20", "年齡 11 ~ 20"),
	type0000010("age21to30", "年齡 21 ~ 30"),
	type0000011("age31to40", "年齡 31 ~ 40"),
	type0000012("age41to50", "年齡 41 ~ 50"),
	type0000013("age51to60", "年齡 51 ~ 60"),
	type0000014("age61to70", "年齡 61 ~ 70"),
	type0000015("age71to80", "年齡 71 ~ 80"),
	type0000016("age81to90", "年齡 81 ~ 90"),
	type0000017("age91to100", "年齡 91 ~ 100"),
	type0000018("ageover100", "年齡超過 100");
	
	private final String key;
	private final String name;

	private CategoryComparisonTableEnum(String key, String name) {
		this.key = key;
		this.name = name;
	}

	public String getKey() {
		return key;
	}

	public String getName() {
		return name;
	}

}
