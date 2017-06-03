package com.pchome.dmp.dao;


import java.util.List;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public interface IPersonalInformationDAO {

	public int insert(List<DBObject> list) throws Exception;

    public int insert(DBObject dbObject) throws Exception;

    public int delete(DBObject dbObject) throws Exception;

    public void deleteAll() throws Exception;

    public int deleteByDate(String dateStr) throws Exception;

    public boolean checkKeyword(String keyword) throws Exception;

    public boolean checkMemIdExist(String memId) throws Exception;

    public boolean checkMemIdAndUuIdExist(String memId, String uuid) throws Exception;

    public DBObject findOnebyMemIdAndUuId(String memId, String uuid) throws Exception;

    public DBCursor customQuery(DBObject query) throws Exception;

    public int customDelete(DBObject query) throws Exception;
}