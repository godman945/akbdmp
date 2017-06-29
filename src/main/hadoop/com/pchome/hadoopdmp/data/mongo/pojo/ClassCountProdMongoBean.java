package com.pchome.hadoopdmp.data.mongo.pojo;


import java.util.Date;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "class_count")
public class ClassCountProdMongoBean {

	private String _id;
	private String memid = "";
	private String uuid = "";
	private String behavior = "";
	private String ad_class = "";
	private String count = "";
	private String record_date = "";
	private Date update_date;
	private Date create_date;

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

	public String getBehavior() {
		return behavior;
	}

	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}

	public String getAd_class() {
		return ad_class;
	}

	public void setAd_class(String ad_class) {
		this.ad_class = ad_class;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getRecord_date() {
		return record_date;
	}

	public void setRecord_date(String record_date) {
		this.record_date = record_date;
	}

	public Date getUpdate_date() {
		return update_date;
	}

	public void setUpdate_date(Date update_date) {
		this.update_date = update_date;
	}

	public Date getCreate_date() {
		return create_date;
	}

	public void setCreate_date(Date create_date) {
		this.create_date = create_date;
	}

	public String get_id() {
		return _id;
	}

}
