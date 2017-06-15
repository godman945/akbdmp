package com.pchome.hadoopdmp.dao;

import java.util.List;

import com.mongodb.DBObject;

public interface IClassUrlDAO {

	public int insert(List<DBObject> list) throws Exception;

    public int insert(DBObject dbObject) throws Exception;

    public int delete(DBObject dbObject) throws Exception;

    public void deleteAll() throws Exception;

    public int deleteByDate(String dateStr) throws Exception;

    public boolean checkKeyword(String keyword) throws Exception;

    public boolean checkUrlExisted(String url) throws Exception;

    public boolean checkUrlClassed(String url) throws Exception;

}