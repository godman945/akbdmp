package com.pchome.dmp.factory.job;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.dmp.dao.IClassCountDAO;
import com.pchome.dmp.dao.IClassUrlDAO;
import com.pchome.dmp.dao.mongodb.ClassCountDAO;
import com.pchome.dmp.dao.mongodb.ClassUrlDAO;
import com.pchome.dmp.enumerate.EnumCategoryJob;
import com.pchome.dmp.enumerate.EnumKdclPvDailyAddedAmount;
import com.pchome.dmp.mapreduce.category.CategoryReducer;

@Component
public class CategoryCountPv extends AncestorJob {

//	private IClassCountDAO dao = ClassCountDAO.getInstance();	//mark by bessie
	protected static int LOG_LENGTH = 5;
	private IClassUrlDAO daoClassUrl = ClassUrlDAO.getInstance();
	
//	private String kafkaMetadataBrokerlist;
//	
//	private String kafkaAcks;
//	
//	private String kafkaRetries;
//	
//	private String kafkaBatchSize;
//	
//	private String kafkaLingerMs;
//	
//	private String kafkaBufferMemory;
//	
//	private String kafkaSerializerClass;
//	
//	private String kafkaKeySerializer;
//	
//	private String kafkaValueSerializer;
	
//	Producer<String, String> producer = null;
	
//	public CategoryCountPv(){
//	System.setProperty("spring.profiles.active", "local");
//	ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//	
//	this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
//	this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
//	this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
//	this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
//	this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
//	this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
//	this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
//	this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
//	this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
//	
//	Properties props = new Properties();
//	props.put("bootstrap.servers", kafkaMetadataBrokerlist);
//	props.put("acks", kafkaAcks);
//	props.put("retries", kafkaRetries);
//	props.put("batch.size", kafkaBatchSize);
//	props.put("linger.ms",kafkaLingerMs );
//	props.put("buffer.memory", kafkaBufferMemory);
//	props.put("serializer.class", kafkaSerializerClass);
//	props.put("key.serializer", kafkaKeySerializer);
//	props.put("value.serializer", kafkaValueSerializer);
//	producer = new KafkaProducer<String, String>(props);
//}


	@Override
	public String getKey(String[] values) {

		if (!"pv".equals(values[13])) {
//			log.info("adType=" + values[13]);
			return null;
		}
		if (values.length < LOG_LENGTH) {
			log.info("Values length is too short");
			return null;
		}

		//memid , uuid
		if( values[1].equals("null") && values[2].equals("null") ) {
			return null;
		}

		//memid, uuid, referer, adClass
//		if( StringUtils.isBlank(values[1]) || StringUtils.isBlank(values[2]) ||
//				StringUtils.isBlank(values[4]) || StringUtils.isBlank(values[15]) ){
//			return null;
//		}

		//parse referer into domain
		String domain = "";
		Pattern p = Pattern.compile("(http|https)://([A-Za-z0-9||_||-]*\\.)*([A-Za-z0-9||_||-]*)/");
		Matcher m = p.matcher(values[4]);
		if( m.find() ) {
			domain = m.group();
			if( domain.charAt(domain.length()-1) == '/' ) {
				domain = domain.substring(0, domain.length()-1);
			}
		}

//		log.info("referer:" + values[4] + "  domain:" + domain);

		String behavior = "";

//		if( values[4].trim().matches("(http|https)://goods.ruten.com.tw/\\S+") ) {
		if( domain.contains("ruten.com.tw") ) {
			behavior = "ruten";
		} else if( domain.contains("24h.pchome.com.tw") ) {
			behavior = "24h";
		} else {
			behavior = "YetDefined";
		}


		StringBuffer sb = new StringBuffer();
		sb.append(EnumCategoryJob.Category_Count_Pv).append(SYMBOL);	//Category_Count_Pv, not package.class
		sb.append(values[1]).append(SYMBOL);					//memId
		sb.append(values[2]).append(SYMBOL);					//uuid
		sb.append(behavior).append(SYMBOL);						//behavior
		sb.append(values[15]);									//adClass

//		log.info(sb.toString());

		return sb.toString();

	}

	@Override
	public String getValue(String[] values) {

		if (!"pv".equals(values[13])) {
//			log.info("adType=" + values[13]);
			return null;
		}

		StringBuffer sb = new StringBuffer();
		sb.append("1").append(SYMBOL);

		String refererUrl = values[4];
        if (StringUtils.isBlank(refererUrl)) {
            sb.append("UNCATED");
            return sb.toString();
        }
        refererUrl = refererUrl.trim();

		if( refererUrl.matches("(http|https)://goods.ruten.com.tw/\\S+")
				|| refererUrl.matches("(http|https)://24h.pchome.com.tw/\\S+") ) {
			try {

				// special for 24h
				if( refererUrl.matches("(http|https)://24h.pchome.com.tw/\\S+") ) {
					Pattern p = Pattern.compile("((http|https)://24h.pchome.com.tw/(store|region)/[a-zA-Z0-9]+)([&|\\?|\\.]\\S*)?");
					Matcher m = p.matcher(refererUrl);
					if( m.find() ) {
						refererUrl = m.group(1);
					} else {
						log.warn("24h, refererUrl no match found");
					}
				}

				if( daoClassUrl.checkUrlClassed(refererUrl) ) {
					sb.append("CATED");
//					log.info("referer matched ruten/24h pattern, and existed in ClassURL");
				} else {
					sb.append(refererUrl);	//referer
//					log.info("referer matched ruten/24h pattern, but not existed in ClassURL");
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		else {
            sb.append("UNCATED");
		}

		return sb.toString();
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

		DBObject doc = null;

		String memid = keys[1];
		String uuid = keys[2];
		String behavior = keys[3];
		String adClass = "";
		if( keys.length==5 ) {
			adClass = keys[4];
		}
		int pv = 0;
		String record_date = CategoryReducer.record_date;

		// count Pv pcid,uuid of cated,uncated
		int catedPcidCnt_ruten = 0;
		int catedUuidCnt_ruten = 0;
		int uncatedPcidCnt_ruten = 0;
		int uncatedUuidCnt_ruten = 0;
		int catedPcidCnt_24h = 0;
		int catedUuidCnt_24h = 0;
		int uncatedPcidCnt_24h = 0;
		int uncatedUuidCnt_24h = 0;

		for (Text text : values) {
			pv++;

			String vals[] = text.toString().split(SYMBOL);
			if( vals.length>1 ) {
				if( vals[1].equals("CATED") ) {		//cated pv
					// count ruten cated pcid、uuid
					if( behavior.equals("ruten") ) {
						if( !memid.equals("null") ) {
							catedPcidCnt_ruten++;
						}
						if( !uuid.equals("null") ) {
							catedUuidCnt_ruten++;
						}
					} else if( behavior.equals("24h") ) {
						if( !memid.equals("null") ) {
							catedPcidCnt_24h++;
						}
						if( !uuid.equals("null") ) {
							catedUuidCnt_24h++;
						}
					}
				}
				if( !vals[1].equals("CATED") ) {	//uncated pv
					outputCollector.add(vals[1]);
//					outputCollectorList.add(vals[1]);

					// count ruten uncated pcid、uuid
					if( behavior.equals("ruten") ) {
						if( !memid.equals("null") ) {
							uncatedPcidCnt_ruten++;
						}
						if( !uuid.equals("null") ) {
							uncatedUuidCnt_ruten++;
						}
					} else if( behavior.equals("24h") ) {
						if( !memid.equals("null") ) {
							uncatedPcidCnt_24h++;
						}
						if( !uuid.equals("null") ) {
							uncatedUuidCnt_24h++;
						}
					}
				}
			} else {
				log.info("Not Cated/UnCated, should be some values[4] not match ruten&24h ....");
			}
		}
//		dailyAddedAmount.put(  "pv_ruten_cated_pcid", dailyAddedAmount.get("pv_ruten_cated_pcid")+catedPcidCnt_ruten );
//		dailyAddedAmount.put( "pv_ruten_cated_uuid", dailyAddedAmount.get("pv_ruten_cated_uuid")+catedUuidCnt_ruten );
//		dailyAddedAmount.put( "pv_ruten_uncated_pcid", dailyAddedAmount.get("pv_ruten_uncated_pcid")+uncatedPcidCnt_ruten );
//		dailyAddedAmount.put( "pv_ruten_uncated_uuid", dailyAddedAmount.get("pv_ruten_uncated_uuid")+uncatedUuidCnt_ruten );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Categorized_pcid_ruten.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Categorized_pcid_ruten.getKeyName())+catedPcidCnt_ruten );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Categorized_uuid_ruten.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Categorized_uuid_ruten.getKeyName())+catedUuidCnt_ruten );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Uncategorized_pcid_ruten.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Uncategorized_pcid_ruten.getKeyName())+uncatedPcidCnt_ruten );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Uncategorized_uuid_ruten.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Uncategorized_uuid_ruten.getKeyName())+uncatedUuidCnt_ruten );

//		dailyAddedAmount.put( "pv_24h_cated_pcid", dailyAddedAmount.get("pv_24h_cated_pcid")+catedPcidCnt_24h );
//		dailyAddedAmount.put( "pv_24h_cated_uuid", dailyAddedAmount.get("pv_24h_cated_uuid")+catedUuidCnt_24h );
//		dailyAddedAmount.put( "pv_24h_uncated_pcid", dailyAddedAmount.get("pv_24h_uncated_pcid")+uncatedPcidCnt_24h );
//		dailyAddedAmount.put( "pv_24h_uncated_uuid", dailyAddedAmount.get("pv_24h_uncated_uuid")+uncatedUuidCnt_24h );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Categorized_pcid_24h.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Categorized_pcid_24h.getKeyName())+catedPcidCnt_24h );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Categorized_uuid_24h.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Categorized_uuid_24h.getKeyName())+catedUuidCnt_24h );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Uncategorized_pcid_24h.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Uncategorized_pcid_24h.getKeyName())+uncatedPcidCnt_24h );
		dailyAddedAmount.put(EnumKdclPvDailyAddedAmount.Uncategorized_uuid_24h.getKeyName(),
				dailyAddedAmount.get(EnumKdclPvDailyAddedAmount.Uncategorized_uuid_24h.getKeyName())+uncatedUuidCnt_24h );

		// Except ruten&24h (20160906)
		if( behavior.equals("YetDefined") ) {
			return null;
		}

		// Now this may be empty (20160907)
		if( StringUtils.isBlank(adClass) ) {
			return null;
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
		doc.put("count", pv);
		doc.put("record_date", record_date);
		doc.put("update_date", new Date());
		doc.put("create_date", new Date());

		return doc;
	}

	@Override
	public void update() {
		try {
//			log.info("list size:"+list.size());
			if (list.size() > LIMIT) {
//				dao.insert(list);//mark by bessie

//				JSONObject sendKafkaJson = new JSONObject();
//				sendKafkaJson.put("type", "mongo");
//				sendKafkaJson.put("action", "insert");
//				sendKafkaJson.put("collection", "class_count");
//				sendKafkaJson.put("column", "");
//				sendKafkaJson.put("condition", "");
//				sendKafkaJson.put("content",list.toString());
//				kafkaList.add(sendKafkaJson);
//				
//				Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString())); // "reducer pv ok"
//				while (!f.isDone()) {
//				}
				list = new ArrayList<DBObject>();
			}
		} catch (Exception e) {
			log.error("mongodb update error= " + e);
		}

	}

	@Override
	public int insert() throws Exception {
//	mark by bessie
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

