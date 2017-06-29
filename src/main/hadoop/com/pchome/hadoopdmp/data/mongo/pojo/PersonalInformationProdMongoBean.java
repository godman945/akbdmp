package com.pchome.hadoopdmp.data.mongo.pojo;

import java.util.Date;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "personal_information")
public class PersonalInformationProdMongoBean {
	private String _id;
	private String memid;
	private String uuid;
	private String age;
	private String sex;
	private String ip_area;
	private Date create_date;
	private Date update_date;

	public String getMemid() {
		return memid;
	}

	public void setMemid(String memid) {
		this.memid = memid;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getIp_area() {
		return ip_area;
	}

	public void setIp_area(String ip_area) {
		this.ip_area = ip_area;
	}

	public Date getCreate_date() {
		return create_date;
	}

	public void setCreate_date(Date create_date) {
		this.create_date = create_date;
	}

	public Date getUpdate_date() {
		return update_date;
	}

	public void setUpdate_date(Date update_date) {
		this.update_date = update_date;
	}

	public String get_id() {
		return _id;
	}

}
