package com.pchome.dmp.factory.job;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pchome.dmp.dao.IClassCountDAO;
import com.pchome.dmp.dao.mongodb.ClassCountDAO;
import com.pchome.dmp.enumerate.EnumCategoryJob;
import com.pchome.dmp.mapreduce.category.CategoryReducer;

@Component
public class CategoryCountCk extends AncestorJob {

	private IClassCountDAO dao = ClassCountDAO.getInstance();	//mark by bessie
	
	protected static int LOG_LENGTH = 5;
	
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
	
	
//	public CategoryCountCk(){
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		
//		this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
//		this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
//		this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
//		this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
//		this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
//		this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
//		this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
//		this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
//		this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
//		
//		Properties props = new Properties();
//		props.put("bootstrap.servers", kafkaMetadataBrokerlist);
//		props.put("acks", kafkaAcks);
//		props.put("retries", kafkaRetries);
//		props.put("batch.size", kafkaBatchSize);
//		props.put("linger.ms",kafkaLingerMs );
//		props.put("buffer.memory", kafkaBufferMemory);
//		props.put("serializer.class", kafkaSerializerClass);
//		props.put("key.serializer", kafkaKeySerializer);
//		props.put("value.serializer", kafkaValueSerializer);
//		producer = new KafkaProducer<String, String>(props);
//	}

	@Override
	public String getKey(String[] values) {

		if (!"ck".equals(values[13])) {
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

		String behavior = "ad_click";

//		if( domain.contains("ruten.com.tw") ) {
//			behavior = "ad_click";
//		} else {
//			behavior = "YetDefined";
//		}


		StringBuffer sb = new StringBuffer();
		sb.append(EnumCategoryJob.Category_Count_Ck).append(SYMBOL);	//Category_Count_Ck, not package.class
		sb.append(values[1]).append(SYMBOL);					//memId
		sb.append(values[2]).append(SYMBOL);					//uuid
		sb.append(behavior).append(SYMBOL);						//behavior
		sb.append(values[15]);									//adClass

		return sb.toString();

	}

	@Override
	public String getValue(String[] values) {

		if (!"ck".equals(values[13])) {
//			log.info("adType=" + values[13]);
			return null;
		}

		return "1";

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
		int ck = 0;
		String record_date = CategoryReducer.record_date;

		for (Text text : values) {
			ck++;
//			log.info("memid:" + keys[1] + " uuid:" + keys[2] + " behavior:" + keys[3] + " adClass:" + keys[4]);
		}

		// count Ck pcid,uuid
		int pcidCnt = 0;
		int uuidCnt = 0;
		if( !memid.equals("null") ) {
			pcidCnt = ck;
		}

		if( !uuid.equals("null") ) {
			uuidCnt = ck;
		}
		dailyAddedAmount.put( "ck_cated_pcid", dailyAddedAmount.get("ck_cated_pcid")+pcidCnt );
		dailyAddedAmount.put( "ck_cated_uuid", dailyAddedAmount.get("ck_cated_uuid")+uuidCnt );

		// Except ruten (20160503)
//		if( behavior.equals("YetDefined") ) {
//			return null;
//		}

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
		doc.put("count", ck);
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
				dao.insert(list);//mark by bessie
				
//				JSONObject sendKafkaJson = new JSONObject();
//				sendKafkaJson.put("type", "mongo");
//				sendKafkaJson.put("action", "insert");
//				sendKafkaJson.put("collection", "class_count");
//				sendKafkaJson.put("column", "");
//				sendKafkaJson.put("condition", "");
//				sendKafkaJson.put("content",list.toString());
//				kafkaList.add(sendKafkaJson);
//				
//				Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "",kafkaList.toString() )); // "reducer ck ok"
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
        if (list.size() <= 0) {
            return 0;
        }

        int count = dao.insert( list );
        list = new ArrayList<DBObject>();

        return count;
//		  return 0;
	}

}
