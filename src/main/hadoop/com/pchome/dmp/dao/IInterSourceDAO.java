package com.pchome.dmp.dao;

import java.util.List;

import com.mongodb.DBObject;

public interface IInterSourceDAO {
    public int insert(List<DBObject> list) throws Exception;
    
    public int insert(DBObject dbObject) throws Exception;

    public int delete(DBObject dbObject) throws Exception;
    
    public void deleteAll() throws Exception;
    
    public int deleteByDate(String fromDate, String toDate) throws Exception;
    
    public boolean checkKeyword(String keyword) throws Exception;

    public int findCountByDate(String fromDate, String toDate) throws Exception;
    
    public List <DBObject> findByDate(int start, int limit, String fromDate, String toDate, DBObject sort) throws Exception;
}