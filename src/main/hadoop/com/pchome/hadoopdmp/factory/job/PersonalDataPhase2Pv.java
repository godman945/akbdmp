package com.pchome.hadoopdmp.factory.job;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.Producer;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.dao.IPersonalInformationDAO;
import com.pchome.hadoopdmp.dao.mongodb.PersonalInformationDAO;
import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase2Job;
import com.pchome.hadoopdmp.mapreduce.prsnldata.PersonalDataPhase2Mapper;
import com.pchome.hadoopdmp.mapreduce.prsnldata.PersonalDataPhase2Mapper.combinedValue;


public class PersonalDataPhase2Pv extends AncestorJob {

	protected static int LOG_LENGTH = 5;
	private IPersonalInformationDAO daoPrsnlInfo = PersonalInformationDAO.getInstance();


	@Override
	public String getKey(String[] values) {

		// memid
//		if( StringUtils.isBlank(values[0]) ) {
//			log.warn("memid is Blank");
//			return null;
//		}

		StringBuffer sb = new StringBuffer();
		sb.append(EnumPersonalDataPhase2Job.Personal_Data_Phase2_Pv).append(SYMBOL);	//package.class
		sb.append(values[0]);		//memId
		sb.append(SYMBOL);
		sb.append(values[1]);		//uuid

		return sb.toString();
	}

	@Override
	public String getValue(String[] values) {

//		if( StringUtils.isBlank(values[0]) ) {
//			return null;
//		}
		String memid = values[0];
		String uuid = values[1];
		String ad_class = "";
		if( values.length>2 )
			ad_class = values[2];

		log.info("memid:" + memid + "  uuid:" + uuid + "  ad_class:" + ad_class);
//		log.info("PersonalDataPhase2Mapper.clsfyCraspMap size:" + PersonalDataPhase2Mapper.clsfyCraspMap.size());	//ok

		/*--- memid is Blank ---*/
		if( (memid.equals("") || memid.equals("null")) && ad_class.matches("\\d{16}") ) {

//			if( true ) {
//				return null;
//			}

			memid = "";
			combinedValue combineObj = PersonalDataPhase2Mapper.clsfyCraspMap.get(ad_class);
			if( combineObj==null ) {
				log.info("combineObj is null, cause no crsp to clsfyCraspMap");
				return null;
			}

			try {
				// here if( (memid&uuid).exist in mongo ), it must be from Large
				// TODO read once, into map
				if( !daoPrsnlInfo.checkMemIdAndUuIdExist(memid, uuid) ) {

					DBObject dbObj = new BasicDBObject();
					dbObj.put("memid", memid);
					dbObj.put("uuid", uuid);
					Date date = new Date();
					dbObj.put("age", combineObj.age);
					dbObj.put("sex", combineObj.gender);
					dbObj.put("ip_area", "");
					dbObj.put("update_date", date);
					dbObj.put("create_date", date);


					int result = 0;
					result = daoPrsnlInfo.insert(dbObj);
					log.info("insert dbObj result:" + result + "  dbObj:" + dbObj.toString());
					log.info("This process has been completed");

//					log.info("ready to insert dbObj:" + dbObj.toString());
//					return dbObj.toString();
				} else {
					log.info("does not deal, cause: memid&uuid already exists in mongo");
					return null;
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}

		} else {
		/*--- memid&uuid is not Blank ---*/
			// MemAPI
			StringBuffer url = new StringBuffer()
					.append("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id=")
					.append(memid);

			String prsnlData = httpGet(url.toString());
			if( StringUtils.isBlank(prsnlData) ) {
				log.warn("memid:" + memid + " 's Personal Data is Blank.");
				return null;
			}

			Object o = com.mongodb.util.JSON.parse( prsnlData );
			DBObject dbObj = (DBObject) o;

			String birthday = (String) dbObj.get("birthday");
			String sexuality = (String) dbObj.get("sexuality");
			String address = (String) dbObj.get("address");

			if( StringUtils.isBlank(birthday) && StringUtils.isBlank(sexuality) && StringUtils.isBlank(address) ) {
	//			log.info("memid:" + memid + " does not have the Personal Data can be analyzed");
				return null;
			}

			// age
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			int age = -1;
			if( StringUtils.isNotBlank(birthday) ) {
				try {
					age = getAge(sdf.parse(birthday));
				} catch (ParseException e) {
					log.warn(e.getMessage());
				}
			}

			// sex
			String sex = StringUtils.isBlank(sexuality)?"":sexuality;

	//		log.info("address:" + address);
			// ip_area
			String ip_area = "";
			String area = "";
			boolean unParsedAreaFlag = false;
			if( StringUtils.isNotBlank(address) ) {
				Pattern p = Pattern.compile("([^a-zA-Z_0-9 ]{2}[\u7e23\u5e02])([\\S]*)");
				Matcher m = p.matcher(address);
				if( m.find() ) {
					area = m.group(1);
	//				log.info("area:" + area);

					if( BelongingToTaiwanCitiesOrCounties(area) ) {
						ip_area = area;
					} else {
						unParsedAreaFlag = true;
						log.info("area:" + area + " not belong to Taiwan (memid:" + memid + ")");
					}
				}

			}
	//		log.info("ip_area:" + ip_area);

			BasicDBObject docMemAPI = new BasicDBObject()
					.append("memid", memid)
					.append("uuid", uuid)
					.append("age", String.valueOf( age>=0?age:"" ))
					.append("sex", sex)
					.append("ip_area", ip_area);

			if( unParsedAreaFlag ) {
				docMemAPI.append("unParsedArea", area);
			}
			log.info("docMemAPI(bf):" + docMemAPI);

			//phase5: compare with from Mongo & memAPI, to complement it, judge need to insert/update/no
			//phase5: if( "neededOperation", "update" ) { remove from mongodb }
			BasicDBList and = new BasicDBList();
			and.add( new BasicDBObject("memid", memid) );
			and.add( new BasicDBObject("uuid", uuid) );
//			DBCursor cursor;
			BasicDBObject docMongo = null;
			String neededOperation = "no";
			boolean existInMongo = false;
			final boolean theSameToUpdateFlag = false;
			try {
				docMongo = (BasicDBObject) daoPrsnlInfo.findOnebyMemIdAndUuId(memid, uuid);
				if( docMongo!=null ) {
					existInMongo = true;
				}

//				cursor = daoPrsnlInfo.customQuery( new BasicDBObject("$and", and) );
//				while( cursor.hasNext() ) {
//					docMongo = (BasicDBObject) cursor.next();
//					existInMongo = true;
//					break;
//				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			if( docMongo==null )
				return null;

			log.info("docMongo:" + docMongo);

			if( !existInMongo ) {
				neededOperation = "insert";
			} else {
				// compare & complement : age, sex, ip_area
				String[] toCpr = {"age", "sex", "ip_area"};
				for(String cprStr:toCpr) {
					String memStr = docMemAPI.get(cprStr).toString();
					String mongoStr = docMongo.get(cprStr).toString();
					if( memStr.equals(mongoStr) ) {
						continue;
					} else {
						neededOperation = "update";
						if( StringUtils.isBlank(memStr) ) {
							docMemAPI.put(cprStr, mongoStr);
						}
					}
				}
			}

			if( theSameToUpdateFlag ) {
				if( neededOperation.equals("no") )
					neededOperation = "update";
			}

			docMemAPI.put("neededOperation", neededOperation);

			// remove for update , and keep create_date
			if( neededOperation.equals("update") ) {
				try {
					int deleteCnt = 0;
					deleteCnt = daoPrsnlInfo.customDelete(new BasicDBObject("$and", and));
					log.info("update -> deleteCnt:" + deleteCnt);
				} catch (Exception e) {
					log.error(e.getMessage());
				}

				docMemAPI.put("create_date", docMongo.get("create_date"));
			}
			docMemAPI.put("update_date", new Date());

			log.info("docMemAPI(af):" + docMemAPI);
			log.info("neededOperation:" + neededOperation);

			return docMemAPI.toString();
		}

		return null;
	}

	@Override
	public void add(String[] keys, Iterable<Text> values) {
		DBObject doc = (DBObject)this.getObject(keys, values);

//		log.info("add list:"+doc);
		if (doc == null) {
			return;
		}
		list.add(doc);
	}

	@Override
	public Object getObject(String[] keys, Iterable<Text> values) {

		String memid = keys[1];
		String uuid = keys[2];
		String prsnlData = "";
		for (Text text : values) {
			if( StringUtils.isNotBlank(text.toString()) ) {
				prsnlData = text.toString();
			}
		}
		if( StringUtils.isBlank(prsnlData) ) {
			log.warn("memid:" + memid + " uuid:" + uuid + " prsnlData is Blank");
			return null;
		}

		Object o = com.mongodb.util.JSON.parse( prsnlData );
		DBObject dbObj = (DBObject) o;

//		dbObj.put("update_date", new Date());
//		dbObj.put("create_date", new Date());

		// record cannot be parsed area
		if( dbObj.containsField("unParsedArea") ) {
			outputCollector.add( (String)dbObj.get("unParsedArea") );
			dbObj.removeField("unParsedArea");
		}

		//phase5: if containsField("neededOperation") { get field, if insert/update memUnCatedCnt(AncestorJob)++ if no memCatedCnt(AncestorJob)++, return null(dont write this record to db}
		if( dbObj.containsField("neededOperation") ) {
			String neededOperation = dbObj.get("neededOperation").toString();
			if( neededOperation.equals("insert") || neededOperation.equals("update") ) {
				memUnCatedCnt++;
			}
			if( neededOperation.equals("no") ) {
				memCatedCnt++;
				return null;
			}
			dbObj.removeField("neededOperation");
		}

		return dbObj;
	}

	@Override
	public void update() {
		try {
			log.info("list size:"+list.size());
			if (list.size() > LIMIT) {
				daoPrsnlInfo.insert(list);
				list = new ArrayList<DBObject>();
			}
		} catch (Exception e) {
			log.error("mongodb update error= " + e);
		}

	}

	@Override
	public int insert() throws Exception {
		return 0;
	}


	public String httpGet(String myURL) {
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(myURL);
			urlConn = url.openConnection();
			if (urlConn != null)
				urlConn.setReadTimeout(60 * 1000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(),
						"UTF-8");
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
		in.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		return sb.toString();
	}

	public int getAge(Date birthDay) {
		Calendar cal = Calendar.getInstance();

		if (cal.before(birthDay)) {
			return -1;
		}

		int yearNow = cal.get(Calendar.YEAR);
		int monthNow = cal.get(Calendar.MONTH)+1;
		int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);

		cal.setTime(birthDay);
		int yearBirth = cal.get(Calendar.YEAR);
		int monthBirth = cal.get(Calendar.MONTH)+1;
		int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);

		int age = yearNow - yearBirth;

		if (monthNow <= monthBirth) {
			if (monthNow == monthBirth) {
				if (dayOfMonthNow < dayOfMonthBirth) {
					age--;
				}
			} else {
				age--;
			}
		}

		return age;
	}

	public boolean BelongingToTaiwanCitiesOrCounties(String area) {
		List<String> list = new ArrayList<String>();
		list.add("臺北市");
		list.add("台北市");
		list.add("新北市");
		list.add("桃園市");
		list.add("臺中市");
		list.add("台中市");
		list.add("臺南市");
		list.add("台南市");
		list.add("高雄市");

		list.add("基隆市");
		list.add("新竹市");
		list.add("嘉義市");

		list.add("新竹縣");
		list.add("苗栗縣");
		list.add("彰化縣");
		list.add("南投縣");
		list.add("雲林縣");
		list.add("嘉義縣");
		list.add("屏東縣");
		list.add("宜蘭縣");
		list.add("花蓮縣");
		list.add("臺東縣");
		list.add("台東縣");
		list.add("澎湖縣");
		list.add("金門縣");
		list.add("連江縣");

		list.add("桃園縣");
		list.add("臺北縣");
		list.add("台北縣");
		list.add("高雄縣");
		list.add("花蓮市");
		list.add("彰化市");
		list.add("宜蘭市");
		list.add("南投市");
		list.add("台東市");
		list.add("屏東市");
		list.add("苗栗市");


		if( list.contains(area) ) {
			return true;
		}
		return false;
	}
}

