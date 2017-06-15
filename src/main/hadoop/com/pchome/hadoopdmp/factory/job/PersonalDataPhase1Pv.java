package com.pchome.hadoopdmp.factory.job;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.Producer;

import com.pchome.hadoopdmp.dao.IPersonalInformationDAO;
import com.pchome.hadoopdmp.dao.mongodb.PersonalInformationDAO;
import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase1Job;


public class PersonalDataPhase1Pv extends AncestorJob {

	protected static int LOG_LENGTH = 5;
	private IPersonalInformationDAO daoPrsnlInfo = PersonalInformationDAO.getInstance();


	@Override
	public String getKey(String[] values) {

		if (!"pv".equals(values[13])) {
//			log.info("adType=" + values[13]);
			return null;
		}

		//memid uuid
//		if( StringUtils.isBlank(values[1]) || values[1].equals("null") ||
//				StringUtils.isBlank(values[2]) || values[2].equals("null") ) {
//			log.warn("memid is Blank");
		if( StringUtils.isBlank(values[2]) || values[2].equals("null") ) {
			return null;
		}

		StringBuffer sb = new StringBuffer();
		sb.append(EnumPersonalDataPhase1Job.Personal_Data_Phase1_Pv).append(SYMBOL);	//package.class
		sb.append(values[1]);		//memId
		sb.append(SYMBOL);
		sb.append(values[2]);		//uuid

		return sb.toString();
	}

	@Override
	public String getValue(String[] values) {

		if (!"pv".equals(values[13])) {
//			log.info("adType=" + values[13]);
			return null;
		}

		StringBuffer sb = new StringBuffer();
//		sb.append("1");
		if( StringUtils.isNotBlank(values[15]) && values[15].trim().matches("\\d{16}") )
			sb.append(values[15].trim());	//ad_class
		else
			sb.append("1");

		return sb.toString();
	}

	@Override
    public void add(String[] keys, Iterable<Text> values) {
//		DBObject doc = (DBObject)this.getObject(keys, values);
		String memid = (String)this.getObject(keys, values);

//		log.info("add list:"+memid);
        if (memid == null) {
            return;
        }

    }

	@Override
	public Object getObject(String[] keys, Iterable<Text> values) {

		String memid = keys[1];
		String uuid = keys[2];

		Map<String,Integer> map = new HashMap<String,Integer>();
		try {
			for (Text text : values) {
				String str = text.toString();
				if( str.matches("\\d{16}") ) {
					if( map.containsKey(str) ) {
						Integer a = map.get(str);
						map.put(str, (a+1));	//no:a++
					} else {
						map.put(str, 1);
					}
				}
			}

			Integer maxAdclassTimes = 0;
			String maxAdclass = "";
			if( map.size()>0 ) {
				for(Map.Entry<String, Integer> entry:map.entrySet()) {
					if( entry.getValue()>maxAdclassTimes ) {
						maxAdclassTimes = entry.getValue();
						maxAdclass = entry.getKey();
					}
				}
			}

			log.info("memid:" + memid + "  uuid:" + uuid + "  maxAdclassTimes:" + maxAdclassTimes + "  maxAdclass:" + maxAdclass);

			if( StringUtils.isNotBlank(maxAdclass) ) {
				outputCollector.add(memid + SYMBOL + uuid + SYMBOL + maxAdclass);
			} else {
				outputCollector.add(memid + SYMBOL + uuid);
			}


//			if( daoPrsnlInfo.checkMemIdAndUuIdExist(memid, uuid) ) {
////				log.info("memid:" + keys[1] + " already exists");
//				outputCollector.add("memCated" + SYMBOL + memid + SYMBOL + uuid);
//				return null;
//			} else {
//				outputCollector.add(memid + SYMBOL + uuid);
//			}
		} catch(Exception e) {
			log.error(e.getMessage());
		}
		return memid;

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

