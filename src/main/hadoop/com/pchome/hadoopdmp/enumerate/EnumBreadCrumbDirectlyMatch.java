package com.pchome.hadoopdmp.enumerate;


public enum EnumBreadCrumbDirectlyMatch {

	type001("\u98fe\u54c1", "0008000000000000"),
	type002("\u5bb6\u7528\u6e05\u6f54\u5291", "0016023400000000"),
	type003("\u9059\u63a7", "0019029100000000"),
	type004("\u8fa6\u516c\u8a2d\u5099", "0021032100000000"),
	type005("\u7cbe\u54c1", "0008014614840000"),

	type006("\u98fe\u54c1", "0008000000000000"),
	type007("\u8eca\u7528\u96fb\u5b50", "0006010011210000"),
	type008("\u7db2\u8def\u786c\u789f", "0001000000000000"),
	type009("\u7db2  \u8def", "0001000000000000"),
	type010("USB\u5468\u908a", "0001000000000000"),

	type011("\u5bf5\u7269\u767e\u8ca8", "0016023100000000"),
	type012("\u96fb\u7af6\u5c08\u5340", "0001000000000000"),
	type013("\u5a66\u5e7c\u5c08\u5340", "0012000000000000"),
	type014("\u81c9\u90e8\u5c08\u5340", "0011016700000000"),
	type015("\u6383\u9664\u7528\u5177", "0016023400000000"),

	type016("\u6c90\u6d74\u4e73", "0016023821870000"),
	type017("Android \u6bbc\\/\u5957", "0002005907680000"),
	type018("\u958b\u67b6\u54c1\u724c", "0007011100000000"),
	type019("\u8349\u672c\u54c1\u724c", "0007011100000000"),
	type020("Nextbit \\/ \u5176\u4ed6", "0002000000000000"),

	type021("\u4f11 \u9592 \u96f6 \u98df", "0017026500000000"),
	type022("\u624b \u5de5 \u7682", "0016023700000000"),
	type023("\u91ab\u7642\u7528\u54c1", "0019029623850000"),
	type024("\\S+[ ]*\u540b", "0001000000000000"),
	type025("DIY\u96fb\u7af6", "0001000000000000"),

	type026("\u885b \u751f \u68c9", "0011017016200000"),
	type027("\u885b \u751f \u7d19", "0016024000000000"),
	type028("\u8fa6\u516c\u50a2\u4ff1", "0021032124260000"),
	type029("\u50a2\u4ff1", "0016024300000000"),
	type030("\u7ae5\u50a2\u5be2", "0016024322290000"),

	type031("\u5bf5\u7269\u98df\u54c1", "0016023100000000"),
	type032("\u6709\u6a5f\u65e5\u7528", "0017026100000000"),
	type033("\u65e5\u7cfb\u7f8e\u599d", "0011000000000000"),
	type034("\u8fa6\u516c\u54c1", "0021032100000000"),
	type035("\u5bb6\u5ead\u5287\u9662", "0004009010390000"),

	type036("\u651d\u5f71\u5305\\/ \u8173\u67b6", "0003000000000000"),
	type037("\u7f8e\u5bb9\u5de5\u5177", "0011017100000000"),
	type038("\u5149\u789f\u7247", "0025038800000000"),
	type039("\u96fb\u8996\u76d2", "0004007409060000"),
	type040("\u5546 \u7528", "0001003700000000"),

	type041("\u55ae\u8eca\u9031\u908a", "0006010100000000"),
	type042("\u539f\u5275\u54c1\u724c", "0007011100000000"),
	type043("\u53f0\u7063\u7cbe\u54c1", "0001000000000000"),

	;

	private String matchPattern;
	private String adClass;

	private EnumBreadCrumbDirectlyMatch(String matchPattern, String adClass) {
		this.matchPattern = matchPattern;
		this.adClass = adClass;
	}

	public String getMatchPattern() {
		return matchPattern;
	}

	public String getAdClass() {
		return adClass;
	}

}
