package com.pchome.hadoopdmp.dao.mongodb;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.dao.IOrgUserDetailDAO;

public class OrgUserDetailDAO extends BaseDAO implements IOrgUserDetailDAO {

	protected Log log = LogFactory.getLog(this.getClass());

	private static String COLLECTION = "user_detail";

	private static OrgUserDetailDAO instance = new OrgUserDetailDAO();

	private OrgUserDetailDAO() {};

	public static IOrgUserDetailDAO getInstance() {
		return instance;
	}

	@Override
	public int insert(List<DBObject> list) throws Exception {
		return insert(list, COLLECTION);
	}

	@Override
	public int insert(DBObject dbObject) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delete(DBObject dbObject) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deleteAll() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean checkKeyword(String keyword) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean checkUrlExisted(String url) throws Exception {
		DBObject query = new BasicDBObject("url", url);
		String collection = "class_url";
		DBObject result = findOneByCustomQuery(query, collection);
		if( result==null )
			return false;
		return true;
	}
	
	@Override
	public DBObject checkUrl(String url) throws Exception {
		DBObject query = new BasicDBObject("url", url);
		String collection = "class_url";
		DBObject result = findOneByCustomQuery(query, collection);
		return result;
	}
	
	@Override
	public boolean checkUrlClassed(String url) throws Exception {
//		int start = 0;
//		int limit = 0;
////		DBObject query = new BasicDBObject("url", url).append("status", "0");	//older
//		DBObject query = new BasicDBObject("url", url);
//		DBObject sort = new BasicDBObject("url", 1);
//		String collection = "class_url";
//
//		List <DBObject> results = findByDate(start, limit, query, sort, collection);
//
//		if( results.size()>0 )
//			return true;
//		return false;

//		DBObject query = new BasicDBObject("url", url);
		DBObject clause1 = new BasicDBObject("url", url);
		DBObject clause2 = new BasicDBObject("status", "1");
		BasicDBList and = new BasicDBList();
		and.add(clause1);
		and.add(clause2);
		DBObject query = new BasicDBObject("$and", and);

		String collection = "class_url";
		DBObject result = findOneByCustomQuery(query, collection);
		if( result==null )
			return false;
		return true;
	}

	@Override
	public int deleteByDate(String dateStr) throws Exception {

    	DBObject query = new BasicDBObject("record_date", dateStr);

    	int result = deleteByDate(query, COLLECTION);

    	return result;

	}
	
	
	@Override
	public DBObject findOneUserDetail(String userId) throws Exception {
		DBObject query = new BasicDBObject("user_id", userId);
		String collection = "user_detail";
		DBObject result = findOneByCustomQuery(query, collection);
		return result;
	}

}