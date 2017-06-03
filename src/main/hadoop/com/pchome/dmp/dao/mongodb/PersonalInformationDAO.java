package com.pchome.dmp.dao.mongodb;


import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.pchome.dmp.dao.IPersonalInformationDAO;

public class PersonalInformationDAO extends BaseDAO implements IPersonalInformationDAO {

	protected Log log = LogFactory.getLog(this.getClass());

	private static String COLLECTION = "personal_information";

	private static PersonalInformationDAO instance = new PersonalInformationDAO();

	private PersonalInformationDAO() {};

	public static IPersonalInformationDAO getInstance() {
		return instance;
	}

	@Override
	public int insert(List<DBObject> list) throws Exception {
		return insert(list, COLLECTION);
	}

	@Override
	public int insert(DBObject dbObject) throws Exception {
		return insert(dbObject, COLLECTION);
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
	public boolean checkMemIdExist(String memId) throws Exception {
		String collection = "personal_information";
		int results = findCountBySpecField("memid", memId, collection);
		if( results>0 )
			return true;
		return false;
	}

	@Override
	public boolean checkMemIdAndUuIdExist(String memid, String uuid) throws Exception {
		String collection = "personal_information";
//		int results = findCountBy2SpecField("memid", memId, "uuid", uuid, collection);

		BasicDBList and = new BasicDBList();
		and.add(new BasicDBObject("memid", memid));
		and.add(new BasicDBObject("uuid", uuid));
		DBObject query = new BasicDBObject("$and", and);

		DBObject dbObj = findOneByCustomQuery(query, collection);
		if( dbObj!=null )
			return true;
		return false;
	}

	@Override
	public DBObject findOnebyMemIdAndUuId(String memid, String uuid) throws Exception {
		String collection = "personal_information";

		BasicDBList and = new BasicDBList();
		and.add(new BasicDBObject("memid", memid));
		and.add(new BasicDBObject("uuid", uuid));
		DBObject query = new BasicDBObject("$and", and);

		return findOneByCustomQuery(query, collection);

	}

	@Override
	public int deleteByDate(String dateStr) throws Exception {
    	DBObject query = new BasicDBObject("record_date", dateStr);
    	int result = deleteByDate(query, COLLECTION);
    	return result;
	}

	@Override
	public DBCursor customQuery(DBObject query) throws Exception {
		return findByCustomQuery(query, COLLECTION);
	}

	@Override
	public int customDelete(DBObject query) throws Exception {
		int result = delete(query, COLLECTION);
		return result;
	}

}