package com.pchome.hadoopdmp.data.mongo.pojo;

import java.util.Date;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "ad_log_class")
public class AdLogClassMongoBean {

	@Indexed
	private String _id;
	private String url = "";
	private String status = "";
	private String ad_class = "";
	private Date create_date;
	private Date update_date;

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

}
