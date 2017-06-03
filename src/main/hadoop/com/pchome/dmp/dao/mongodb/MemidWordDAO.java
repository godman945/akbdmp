package com.pchome.dmp.dao.mongodb;

import java.util.List;

import com.mongodb.DBObject;
import com.pchome.dmp.dao.IMemidWordDAO;
import com.pchome.dmp.enumerate.EnumChannel;

public class MemidWordDAO extends BaseDAO implements IMemidWordDAO {
    private static IMemidWordDAO instance = new MemidWordDAO();
    private static String COLLECTION = EnumChannel.memid.toString();

    private MemidWordDAO() {};

    public static IMemidWordDAO getInstance() {
        return instance;
    }

    @Override
    public int insert(List<DBObject> list) throws Exception {
        return insert(list, COLLECTION);
    }
    
    public int insert(DBObject dbObject) throws Exception{
    	return insert(dbObject, COLLECTION);
    }

    public int delete(DBObject dbObject) throws Exception {
        return delete(dbObject, COLLECTION);
    }
    
    public void deleteAll() throws Exception {
    	deleteAll(COLLECTION);
    }
    
    public boolean checkKeyword(String keyword) throws Exception {
    	DBObject result = findByKeyword(keyword, COLLECTION);
//    	log.info("checkkeyword:" + result);
    	if(result == null){
    		return false;
    	}
    	return true;
    }
}