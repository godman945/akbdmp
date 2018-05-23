package com.pchome.hadoopdmp.data.mongo.pojo;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "user_detail")
public class UserDetailMongoBeanForHadoop {

	private String _id;
//	@Indexed
	private String user_id = "";
	private String create_date = "";
	private String update_date = "";
	private Map<String, String> user_info = new HashMap<String, String>();

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public Map<String, String> getUser_info() {
		return user_info;
	}

	public void setUser_info(Map<String, String> user_info) {
		this.user_info = user_info;
	}

	public String get_id() {
		return _id;
	}

	public String getCreate_date() {
		return create_date;
	}

	public void setCreate_date(String create_date) {
		this.create_date = create_date;
	}

	public String getUpdate_date() {
		return update_date;
	}

	public void setUpdate_date(String update_date) {
		this.update_date = update_date;
	}

}
