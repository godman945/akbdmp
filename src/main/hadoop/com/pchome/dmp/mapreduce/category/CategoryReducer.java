package com.pchome.dmp.mapreduce.category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

import com.mongodb.DBObject;
import com.pchome.dmp.dao.sql.KdclStatisticsSourceDAO;
import com.pchome.dmp.enumerate.EnumCategoryJob;
import com.pchome.dmp.enumerate.EnumKdclCkDailyAddedAmount;
import com.pchome.dmp.enumerate.EnumKdclPvDailyAddedAmount;
import com.pchome.dmp.factory.job.AncestorJob;
import com.pchome.dmp.factory.job.FactoryCategoryJob;
import com.pchome.soft.depot.utils.KafkaUtil;

@Component
public class CategoryReducer extends Reducer<Text, Text, Text, Text> {

	private final static String SYMBOL = String.valueOf(new char[]{9, 31});
//	private Log log = LogFactory.getLog(this.getClass());
	
	private static Log log = LogFactory.getLog("hadoop_CategoryDriver");

	public static String record_date;

	public AncestorJob job = null;
	
//	Producer<String, String> producer = null;

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		//log.info("record_date: " + record_date);
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "kafka1.mypchome.com.tw:9091");
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 536870912);
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		producer = new KafkaProducer<String, String>(props);

		try {
			for( EnumCategoryJob enumCategoryJob: EnumCategoryJob.values() ) {
				AncestorJob job = FactoryCategoryJob.getInstance( enumCategoryJob );
//				if( enumCategoryJob.getClassName().equals("com.pchome.dmp.factory.job.CategoryCountCk") ) {
				if( enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Ck.getClassName()) ) {
					for( EnumKdclCkDailyAddedAmount enumKdclCkDailyAddedAmount: EnumKdclCkDailyAddedAmount.values() ) {		//Categorized_pcid,Categorized_uuid
						job.dailyAddedAmount.put( enumKdclCkDailyAddedAmount.getKeyName(), new Integer(0));
					}
				}
//				if( enumCategoryJob.getClassName().equals("com.pchome.dmp.factory.job.CategoryCountPv") ) {
				if( enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Pv.getClassName()) ) {
					for( EnumKdclPvDailyAddedAmount enumKdclPvDailyAddedAmount: EnumKdclPvDailyAddedAmount.values() ) {
						job.dailyAddedAmount.put( enumKdclPvDailyAddedAmount.getKeyName(), new Integer(0));
					}
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {

		try {
//			log.info("key=" + key);

			if (StringUtils.isBlank(key.toString())) {
				log.info("key is blank");
				return;
			}

			String[] keys = key.toString().split(SYMBOL);

			EnumCategoryJob enumCategoryJob = EnumCategoryJob.valueOf(keys[0]);
			job = FactoryCategoryJob.getInstance(enumCategoryJob);
			job.add(keys, value);
//			job.update();
			
			log.info("---bessie TEST OK------bessie TEST OK------bessie TEST OK------bessie TEST OK------bessie TEST OK---");

		} catch (Exception e) {
			log.error(key, e);
		}

	}

	@Override
	public void cleanup(Context context) {

		try {
			
			//suspected: Error: GC overhead limit exceeded
			//    		job.update();	//mongoDB

			//test OK
			if( !job.outputCollector.isEmpty() ) {
				for(String str:job.outputCollector) {
					context.write(new Text(str.trim()), null );
				}
				job.outputCollector.clear();
			}
//			
			
			//bessie-start
			//list send kafka--add by bessie
			KafkaUtil kafkaUtil = new KafkaUtil();
			List<JSONObject> kafka = new ArrayList<>();
			JSONObject sendKafkaJson = new JSONObject();
			
			for (EnumCategoryJob enumCategoryJob : EnumCategoryJob.values()) { // Category_Count_Ck,Category_Count_Pv
				AncestorJob job = FactoryCategoryJob.getInstance(enumCategoryJob);
				if (enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Ck.getClassName())) {
					sendKafkaJson.put("type", "mongo");
					sendKafkaJson.put("action", "insert");
					sendKafkaJson.put("collection", "class_count");
					sendKafkaJson.put("column", "");
					sendKafkaJson.put("condition", "");
					sendKafkaJson.put("Test", "ck ck ck");
					sendKafkaJson.put("content", job.list.toString());
					kafka.add(sendKafkaJson);
					kafkaUtil.sendMessage("TEST", "", kafka.toString());
//					for (DBObject jsonObject : job.list) {
//						sendKafkaJson.put("type", "mongo");
//						sendKafkaJson.put("action", "insert");
//						sendKafkaJson.put("collection", "class_count");
//						sendKafkaJson.put("column", "");
//						sendKafkaJson.put("condition", "");
//						sendKafkaJson.put("Test", "ck ck ck");
//						sendKafkaJson.put("content", jsonObject.toString());
//						kafka.add(sendKafkaJson);
//						kafkaUtil.sendMessage("TEST", "", kafka.toString());
//					}
					
				}
				
				if (enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Pv.getClassName())) {
					sendKafkaJson.put("type", "mongo");
					sendKafkaJson.put("action", "insert");
					sendKafkaJson.put("collection", "class_count");
					sendKafkaJson.put("column", "");
					sendKafkaJson.put("condition", "");
					sendKafkaJson.put("Test", "PV pV pV");
					sendKafkaJson.put("content", job.list.toString());
					kafka.add(sendKafkaJson);
					kafkaUtil.sendMessage("TEST", "", kafka.toString());
//					for (DBObject jsonObject : job.list) {
//						sendKafkaJson.put("type", "mongo");
//						sendKafkaJson.put("action", "insert");
//						sendKafkaJson.put("collection", "class_count");
//						sendKafkaJson.put("column", "");
//						sendKafkaJson.put("condition", "");
//						sendKafkaJson.put("Test", "PV PV PV");
//						sendKafkaJson.put("content", jsonObject.toString());
//						kafka.add(sendKafkaJson);
//						kafkaUtil.sendMessage("TEST", "", kafka.toString());
//					}
				}
			}
			kafkaUtil.sendMessage("TEST", "", "ck ck ");
			kafkaUtil.sendMessage("TEST", "","PV pV");
			kafkaUtil.close();
			//bessie-end
			
			
			
//			if( !job.outputCollectorList.isEmpty() ) {
//				for(String str:job.outputCollectorList) {
//					context.write(new Text(str.trim()), null );
//				}
//				job.outputCollectorList.clear();
//			}

//			// write cated&uncated ck,pv count to MySQL
//			log.info("****** ******");
//			try {
//				KdclStatisticsSourceDAO dao = new KdclStatisticsSourceDAO();
//				dao.dbInit();
//				dao.deleteByBehaviorAndRecordDate("ad_click", record_date);
//				dao.deleteByBehaviorAndRecordDate("ruten", record_date);
//				dao.deleteByBehaviorAndRecordDate("24h", record_date);
//
//				for( EnumCategoryJob enumCategoryJob: EnumCategoryJob.values() ) {			//Category_Count_Ck,Category_Count_Pv
//					AncestorJob job = FactoryCategoryJob.getInstance( enumCategoryJob );
//					for( Map.Entry<String, Integer> entry:job.dailyAddedAmount.entrySet() ) {		//CK:(2)ck_cated_pcid,ck_cated_uuid PV:(8)
//						log.info(entry.getKey() + "  " + entry.getValue());							//ck_cated_pcid  30
//
//						if( enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Ck.getClassName()) ) {
//							for( EnumKdclCkDailyAddedAmount x: EnumKdclCkDailyAddedAmount.values() ) {
//								if( entry.getKey().equals(x.getKeyName()) ) {
//									log.info(x + ":" + x.getId_type() +":"+ x.getService_type() +":"+
//											x.getBehavior() +":"+ x.getClassify() +":"+ entry.getValue() +":"+ record_date);
//									dao.insert(x.getId_type(), x.getService_type(), x.getBehavior(), x.getClassify(), entry.getValue(), record_date);
//									break;
//								}
//							}
//
////							for( EnumKdclCkDailyAddedAmount x: EnumKdclCkDailyAddedAmount.values() ) {			//Categorized_pcid,Categorized_uuid
////								log.info(x + ":" + x.getId_type() +":"+ x.getService_type() +":"+
////										x.getBehavior() +":"+ x.getClassify() +":"+ entry.getValue() +":"+ record_date);
////								dao.insert(x.getId_type(), x.getService_type(), x.getBehavior(), x.getClassify(), entry.getValue(), record_date);
////							}
//						}
//						if( enumCategoryJob.getClassName().equals(EnumCategoryJob.Category_Count_Pv.getClassName()) ) {
//							for( EnumKdclPvDailyAddedAmount x: EnumKdclPvDailyAddedAmount.values() ) {
//								if( entry.getKey().equals(x.getKeyName()) ) {
//									log.info(x + ":" + x.getId_type() +":"+ x.getService_type() +":"+
//											x.getBehavior() +":"+ x.getClassify() +":"+ entry.getValue() +":"+ record_date);
//									dao.insert(x.getId_type(), x.getService_type(), x.getBehavior(), x.getClassify(), entry.getValue(), record_date);
//									break;
//								}
//							}
//
////							for( EnumKdclPvDailyAddedAmount x: EnumKdclPvDailyAddedAmount.values() ) {
////								log.info(x + ":" + x.getId_type() +":"+ x.getService_type() +":"+
////										x.getBehavior() +":"+ x.getClassify() +":"+ entry.getValue() +":"+ record_date);
////								dao.insert(x.getId_type(), x.getService_type(), x.getBehavior(), x.getClassify(), entry.getValue(), record_date);
////							}
//						}
//					}
//				}
//				dao.closeAll();
//			} catch (Exception e) {
//				log.error(e.getMessage());
//			}
//			log.info("****** ******");

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

}
