package com.pchome.hadoopdmp.mapreduce.prsnldata;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.Producer;

import com.pchome.hadoopdmp.dao.sql.KdclStatisticsSourceDAO;
import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase2Job;
import com.pchome.hadoopdmp.factory.job.AncestorJob;
import com.pchome.hadoopdmp.factory.job.FactoryPersonalDataPhase2;

public class PersonalDataPhase2Reducer extends Reducer<Text, Text, Text, Text> {

	private final static String SYMBOL = String.valueOf(new char[]{9, 31});
	private Log log = LogFactory.getLog(this.getClass());

	public static String record_date;

	public AncestorJob job = null;
	
	Producer<String, String> producer = null;

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		//log.info("record_date: " + record_date);
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			log.info("Phase2 Reducer, key:" + key);
			if (StringUtils.isBlank(key.toString())) {
				log.warn("key is blank");
				return;
			}

			String[] keys = key.toString().split(SYMBOL);
			if( keys.length!=3 ) {
				log.warn("keys length abnormal");
				for(String str:keys) {
					log.warn(str);
				}
			}

			EnumPersonalDataPhase2Job enumPersonalDataPhase2Job = EnumPersonalDataPhase2Job.valueOf(keys[0]);
			job = FactoryPersonalDataPhase2.getInstance(enumPersonalDataPhase2Job);
			job.add(keys, value);
//			job.update();

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	@Override
	public void cleanup(Context context) {
		try {

			//suspected: Error: GC overhead limit exceeded
			//    		job.update();	//mongoDB

			if( job==null ) {
				log.info("job==null, return");
				return;
			}
			job.update();

			if( !job.outputCollector.isEmpty() ) {
				for(String str:job.outputCollector) {
					context.write(new Text(str.trim()), null );
				}
				job.outputCollector.clear();
			}

			// phase5: write personal cated&uncated count to MySQL
			log.info("memUnCatedCnt:" + AncestorJob.memUnCatedCnt);
			log.info("memCatedCnt:" + AncestorJob.memCatedCnt);

			KdclStatisticsSourceDAO dao = new KdclStatisticsSourceDAO();
			dao.dbInit();
			String counterStr = dao.select("memid", "member", "personal_info", "Y", record_date);
			int counterUpdate = AncestorJob.memCatedCnt;
			if( StringUtils.isNotBlank(counterStr) && StringUtils.isNumeric(counterStr) ) {
				dao.delete("memid", "member", "personal_info", "Y", record_date);
				counterUpdate += Integer.parseInt(counterStr);
			}
			dao.insert("memid", "member", "personal_info", "Y", counterUpdate, record_date);		//pcid -> memid		2 -> personal_info

			counterStr = dao.select("memid", "member", "personal_info", "N", record_date);
			counterUpdate = AncestorJob.memUnCatedCnt;
			if( StringUtils.isNotBlank(counterStr) && StringUtils.isNumeric(counterStr) ) {
				dao.delete("memid", "member", "personal_info", "N", record_date);
				counterUpdate += Integer.parseInt(counterStr);
			}
			dao.insert("memid", "member", "personal_info", "N", counterUpdate, record_date);	//pcid -> memid		2 -> personal_info
			dao.closeAll();

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

}
