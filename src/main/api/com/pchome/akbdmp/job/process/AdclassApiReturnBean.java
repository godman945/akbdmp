package com.pchome.akbdmp.job.process;

import java.util.ArrayList;
import java.util.List;

public class AdclassApiReturnBean {
	private String sex = "";
	private List<String> ad_class = new ArrayList<String>();
	private String behavior = "";
	private String age = "";

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public List<String> getAd_class() {
		return ad_class;
	}

	public void setAd_class(List<String> ad_class) {
		this.ad_class = ad_class;
	}

	public String getBehavior() {
		return behavior;
	}

	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

}
