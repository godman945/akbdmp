package com.pchome.hadoopdmp.factory.job;


import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.Producer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.prsnldata.PersonalDataReducer;


public class PersonalDataPhase1Ck extends AncestorJob {

//	private IClassCountDAO dao = ClassCountDAO.getInstance();
//	protected static int LOG_LENGTH = 5;


	@Override
	public String getKey(String[] values) {

		return null;

//		if (!"ck".equals(values[13])) {
////			log.info("adType=" + values[13]);
//			return null;
//		}


	}

	@Override
	public String getValue(String[] values) {

		return null;

//		if (!"ck".equals(values[13])) {
////			log.info("adType=" + values[13]);
//			return null;
//		}
//
//		return "1";

	}

	@Override
    public void add(String[] keys, Iterable<Text> values) {
		DBObject doc = (DBObject)this.getObject(keys, values);

		log.info("add list:"+doc);
        if (doc == null) {
            return;
        }
        list.add(doc);
    }

	@Override
	public Object getObject(String[] keys, Iterable<Text> values) {

		DBObject doc = null;

		String memid = keys[1];
		String uuid = keys[2];
		String behavior = keys[3];
		String adClass = keys[4];
		int ck = 0;
		String record_date = PersonalDataReducer.record_date;

		for (Text text : values) {
			ck++;
		}

		if( memid.equals("null") ) {
			memid = "";
		}
		if( uuid.equals("null") ) {
			uuid = "";
		}

		doc = new BasicDBObject();
		doc.put("memid", memid);
		doc.put("uuid", uuid);
		doc.put("behavior", behavior);
		doc.put("ad_class", adClass);
		doc.put("count", ck);
		doc.put("record_date", record_date);
		doc.put("update_date", new Date());
		doc.put("create_date", new Date());

		return doc;
	}

	@Override
	public void update() {
//		try {
//			log.info("list size:"+list.size());
//			if (list.size() > LIMIT) {
//				dao.insert(list);
//				list = new ArrayList<DBObject>();
//			}
//		} catch (Exception e) {
//			log.error("mongodb update error= " + e);
//		}

	}

	@Override
	public int insert() throws Exception {
//
//        if (list.size() <= 0) {
//            return 0;
//        }
//
//        int count = dao.insert( list );
//        list = new ArrayList<DBObject>();
//
//        return count;
		return 0;
	}

}
