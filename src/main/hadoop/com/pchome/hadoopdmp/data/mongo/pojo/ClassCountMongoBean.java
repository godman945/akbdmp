package com.pchome.hadoopdmp.data.mongo.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "class_count_test")
public class ClassCountMongoBean {

	private String _id;
	@Indexed
	private String user_id = "";
	private String create_date = "";
	private String update_date = "";
	private long query_times = 0;
	private List<Map<String, Object>> category_info = new ArrayList<>();
	private Map<String, Object> user_info = new HashMap<>();

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public long getQuery_times() {
		return query_times;
	}

	public void setQuery_times(long query_times) {
		this.query_times = query_times;
	}

	public List<Map<String, Object>> getCategory_info() {
		return category_info;
	}

	public void setCategory_info(List<Map<String, Object>> category_info) {
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
