package com.pchome.hadoopdmp.dao.mongodb;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.dao.IInterSourceDAO;
import com.pchome.hadoopdmp.enumerate.EnumChannel;

public class InterSourceDAO extends BaseDAO implements IInterSourceDAO {
    private static IInterSourceDAO instance = new InterSourceDAO();
    private static String COLLECTION = EnumChannel.intersource.toString();

    private InterSourceDAO() {};

    public static IInterSourceDAO getInstance() {
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
    
    public int deleteByDate(String fromDate, String toDate) throws Exception {
    	DBObject query = new BasicDBObject();
    	query.put("date", new BasicDBObject("$gte", fromDate));
    	query.put("date", new BasicDBObject("$lt", toDate));
    	int result = deleteByDate(query, COLLECTION);
    	
    	return result;
    }    
    public boolean checkKeyword(String keyword) throws Exception {
    	DBObject result = findByKeyword(keyword, COLLECTION);
//    	log.info("checkkeyword:" + result);
    	if(result == null){
    		return false;
    	}
    	return true;
    }
    
    public int findCountByDate(String fromDate, String toDate) throws Exception {
    	DBObject query = new BasicDBObject();
    	query.put("date", new BasicDBObject("$gte", fromDate));
    	query.put("date", new BasicDBObject("$lt", toDate));
    	int result = findCountByDate(query, COLLECTION);
    	
    	return result;
    }
    
    public List <DBObject> findByDate(int start, int limit, String fromDate, String toDate, DBObject sort) throws Exception {
    	DBObject query = new BasicDBObject();
    	query.put("date", new BasicDBObject("$gte", fromDate));
    	query.put("date", new BasicDBObject("$lt", toDate));

    	List <DBObject> results = findByDate(start, limit, query, sort, COLLECTION);
    	
    	return results;
    }
}