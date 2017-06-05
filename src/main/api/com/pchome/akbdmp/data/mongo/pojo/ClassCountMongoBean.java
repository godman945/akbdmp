package com.pchome.akbdmp.data.mongo.pojo;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "class_count")
public class ClassCountMongoBean {

	private String _id;
	@Indexed
	private String user_id = "";
	private Map<String, Object> category_info = new HashMap<>();
	private Map<String, Object> user_info = new HashMap<>();

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public Map<String, Object> getCategory_info() {
		return category_info;
	}

	public void setCategory_info(Map<String, Object> category_info) {
		this.category_info = category_info;
	}

	public Map<String, Object> getUser_info() {
		return user_info;
	}

	public void setUser_info(Map<String, Object> user_info) {
		this.user_info = user_info;
	}

	public String get_id() {
		return _id;
	}

}
