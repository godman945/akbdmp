package com.pchome.hadoopdmp.dao.mongodb;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.dao.IClassCountDAO;


public class ClassCountDAO extends BaseDAO implements IClassCountDAO {

	private static String COLLECTION = "class_count";

	private static ClassCountDAO instance = new ClassCountDAO();

	public ClassCountDAO() {};

	public static IClassCountDAO getInstance() {
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
	public boolean checkUrlClassed(String url) throws Exception {
		int start = 0;
		int limit = 0;
		DBObject query = new BasicDBObject("url", url).append("flag", "true");
		DBObject sort = new BasicDBObject("url", 1);
		String collection = "class_url";

		List <DBObject> results = findByDate(start, limit, query, sort, collection);

		if( results.size()>0 )
			return true;
		return false;
	}

	@Override
	public int findCountByDate(String fromDateStr, String toDateStr) throws Exception {

    	//Date fromDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2015-11-21 01:00:00");
        //Date toDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2015-11-21 23:00:00");

		Date fromDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(fromDateStr);
        Date toDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(toDateStr);

        DBObject clause1 = new BasicDBObject("create_date", new BasicDBObject("$gte", fromDate));
        DBObject clause2 = new BasicDBObject("create_date", new BasicDBObject("$lt", toDate));
        BasicDBList and = new BasicDBList();

        and.add(clause1);
        and.add(clause2);
    	DBObject query = new BasicDBObject("$and", and);

    	int result = findCountByDate(query, COLLECTION);

    	return result;
    }

	@Override
	public int deleteByDate(String dateStr) throws Exception {

    	DBObject query = new BasicDBObject("record_date", dateStr);

    	int result = deleteByDate(query, COLLECTION);

    	return result;

	}


}
