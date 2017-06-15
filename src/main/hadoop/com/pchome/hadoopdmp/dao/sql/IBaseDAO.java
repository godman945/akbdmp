package com.pchome.hadoopdmp.dao.sql;


public interface IBaseDAO {

	public boolean executeUpdate(String sql, Object[] args);
	
	public boolean executeQuery(String sql, Object[] args);
	
}