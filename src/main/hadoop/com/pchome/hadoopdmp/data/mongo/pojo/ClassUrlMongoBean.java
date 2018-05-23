package com.pchome.hadoopdmp.data.mongo.pojo;

import java.util.Date;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "class_url")
public class ClassUrlMongoBean {

	@Indexed
	private String _id;
	private String url = "";
	private String status = "";
	private String ad_class = "";
	private Date create_date;
	private Date update_date;
	private int query_time;
	private String ruten_bread = "";
	private String err_msg ="";

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getAd_class() {
		return ad_class;
	}

	public void setAd_class(String ad_class) {
		this.ad_class = ad_class;
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

	public int getQuery_time() {
		return query_time;
	}

	public void setQuery_time(int query_time) {
		this.query_time = query_time;
	}

	public String getRuten_bread() {
		return ruten_bread;
	}

	public void setRuten_bread(String ruten_bread) {
		this.ruten_bread = ruten_bread;
	}

	public String getErr_msg() {
		return err_msg;
	}

	public void setErr_msg(String err_msg) {
		this.err_msg = err_msg;
	}
	
}
