package com.pchome.dmp.mapreduce.crawlbreadcrumb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

import net.minidev.json.JSONObject;

@Component
public class CrawlBreadCrumbReducer extends Reducer<Text, Text, Text, Text> {

	private Log log = LogFactory.getLog(this.getClass());

	private String kafkaMetadataBrokerlist;
	
	private String kafkaAcks;
	
	private String kafkaRetries;
	
	private String kafkaBatchSize;
	
	private String kafkaLingerMs;
	
	private String kafkaBufferMemory;
	
	private String kafkaSerializerClass;
	
	private String kafkaKeySerializer;
	
	private String kafkaValueSerializer;
	
	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;
	
	
	/*mark by bessie
	private String host;
    private int port;
    private String dbname;
    private String user;
    private String password;

    private List<DBObject> list = new ArrayList<DBObject>();
    private MongoClient mongoClient;
    private DBCollection coll;
    */

    @Override
    public void setup(Context context) {
/* mark by bessie
    	try{
	    	Properties prop = new Properties();
			prop.load( BaseDAO.class.getResourceAsStream("/config/prop/mongodb.properties") );

			host = prop.getProperty("mongodb.host");
	    	port = Integer.valueOf(prop.getProperty("mongodb.port"));
	    	dbname = prop.getProperty("mongodb.dbname");
	    	user = prop.getProperty("mongodb.user");
	    	password = prop.getProperty("mongodb.password");

	    	MongoCredential credential = MongoCredential.createMongoCRCredential(user, dbname, password.toCharArray());		//.createCredential(userName, database, password);
			mongoClient = new MongoClient(new ServerAddress(host , port), Arrays.asList(credential));

			DB db = mongoClient.getDB( "dmp" );
			coll = db.getCollection("class_url");

    	} catch(Exception e) {
    		log.error(e.getMessage());
    	}
*/
    	try {
    		System.setProperty("spring.profiles.active", "local");
    		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			
			this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
			this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
			this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
			this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
			this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
			this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
			this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
			this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
			this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
			
			Properties props = new Properties();
			props.put("bootstrap.servers", kafkaMetadataBrokerlist);
			props.put("acks", kafkaAcks);
			props.put("retries", kafkaRetries);
			props.put("batch.size", kafkaBatchSize);
			props.put("linger.ms",kafkaLingerMs );
			props.put("buffer.memory", kafkaBufferMemory);
			props.put("serializer.class", kafkaSerializerClass);
			props.put("key.serializer", kafkaKeySerializer);
			props.put("value.serializer", kafkaValueSerializer);
			producer = new KafkaProducer<String, String>(props);
			
		} catch (Exception e) {
			log.error(e.getMessage());
		}
    	
    }

	@Override
    public void reduce(Text key, Iterable<Text> value, Context context) {

//		String database = "admin";
//		String userName = "webuser";
//		String password = "axw2mP1i";

		try {
//			Properties prop = new Properties();
//			prop.load( BaseDAO.class.getResourceAsStream("/config/prop/mongodb.properties") );
//
//			host = prop.getProperty("mongodb.host");
//	    	port = Integer.valueOf(prop.getProperty("mongodb.port"));
//	    	dbname = prop.getProperty("mongodb.dbname");
//	    	user = prop.getProperty("mongodb.user");
//	    	password = prop.getProperty("mongodb.password");
//
//	    	MongoCredential credential = MongoCredential.createMongoCRCredential(user, dbname, password.toCharArray());		//.createCredential(userName, database, password);
//			MongoClient mongoClient = new MongoClient(new ServerAddress(host , port), Arrays.asList(credential));
//
//			//old
////			MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password.toCharArray());		//.createCredential(userName, database, password);
////			MongoClient mongoClient = new MongoClient(new ServerAddress("mgodev.mypchome.com.tw" , 27017), Arrays.asList(credential));
//
//			DB db = mongoClient.getDB( "dmp" );
//			DBCollection coll = db.getCollection("class_url");

			//older
//			List<DBObject> list = new ArrayList<DBObject>();
			
			/*mark by bessie
			BasicDBObject doc;

			log.info("key:" + key.toString());

			for(Text url:value) {
				doc = new BasicDBObject("url", url.toString())
						.append("status", (key.toString().matches("\\d{16}")?"1":"0") )		//(0:未分類  1:已分類  2:跳過)
						.append("ad_class", (key.toString().matches("\\d{16}")?key.toString():"") ).append("update_date", new Date()).append("create_date", new Date());
				log.info("url(value):" + url.toString());
				log.info("doc:" + doc.toString());
				list.add( doc );
			}
			*/
			
			log.info("key:" + key.toString());
			Date date = new Date();
			for (Text url : value) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>" + key);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>" + value);
				
				JSONObject json = new JSONObject();
				json.put("url", url.toString());
				json.put("status", (key.toString().matches("\\d{16}")?"1":"0"));	//(0:未分類  1:已分類  2:跳過)
				json.put("ad_class", key.toString().matches("\\d{16}")?key.toString():"");
				json.put("create_date", date);
				json.put("update_date", date);
				
				kafkaList.add(json);
			}
			
			
			

//			coll.insert(list);	//older
//			mongoClient.close();	//older

//			for(String str: listClassed) {
//				doc = new BasicDBObject("url", str)
//						.append("status", "1").append("ad_class", "CateA").append("update_date", new Date()).append("create_date", new Date());
//				System.out.println("insert result:" + coll.insert(doc) );
//			}

//			for(Text str:value) {
//				context.write( key , str );
//			}
		} catch (Exception e) {
			log.error("error" + e);
		}

	}

	@Override
    public void cleanup(Context context) {

    	try {
    		/*mark by bessie
    		coll.insert(list);
    		mongoClient.close();
    		*/
    		Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString()));
			while (!f.isDone()) {
			}
    		
    	} catch (Exception e) {
			log.error(e.getMessage());
		}

    }

}
