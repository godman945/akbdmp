package com.pchome.dmp.dao;

import java.util.List;

import com.mongodb.DBObject;

public interface IMemidWordDAO {
    public int insert(List<DBObject> list) throws Exception;
    
    public int insert(DBObject dbObject) throws Exception;

    public int delete(DBObject dbObject) throws Exception;
    
    public void deleteAll() throws Exception;
    
    public boolean checkKeyword(String keyword) throws Exception;
}