package com.pchome.dmp.dao.mongodb;

import java.util.List;

import com.mongodb.DBObject;
import com.pchome.dmp.dao.IUuidWordDAO;
import com.pchome.dmp.enumerate.EnumChannel;

public class UuidWordDAO extends BaseDAO implements IUuidWordDAO {
    private static IUuidWordDAO instance = new UuidWordDAO();
    private static String COLLECTION = EnumChannel.uuid.toString();

    private UuidWordDAO() {};

    public static IUuidWordDAO getInstance() {
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