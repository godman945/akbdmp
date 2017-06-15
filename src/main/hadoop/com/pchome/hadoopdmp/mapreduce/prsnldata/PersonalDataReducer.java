package com.pchome.hadoopdmp.mapreduce.prsnldata;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase1Job;
import com.pchome.hadoopdmp.factory.job.AncestorJob;
import com.pchome.hadoopdmp.factory.job.FactoryPersonalData;

public class PersonalDataReducer extends Reducer<Text, Text, Text, Text> {

	private final static String SYMBOL = String.valueOf(new char[]{9, 31});
	private Log log = LogFactory.getLog(this.getClass());

	public static String record_date;

	public AncestorJob job = null;

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		//log.info("record_date: " + record_date);
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {

		try {
//			log.info("key=" + key);

			if (StringUtils.isBlank(key.toString())) {
//				log.info("key is blank");
				return;
			}

			String[] keys = key.toString().split(SYMBOL);

			EnumPersonalDataPhase1Job enumPersonalDataPhase1Job = EnumPersonalDataPhase1Job.valueOf(keys[0]);
			job = FactoryPersonalData.getInstance(enumPersonalDataPhase1Job);
			job.add(keys, value);
//			job.update();

		} catch (Exception e) {
			log.error(key, e);
		}

	}

	@Override
	public void cleanup(Context context) {

		try {

			//suspected: Error: GC overhead limit exceeded
			//    		job.update();	//mongoDB

//			int memCatedCnt = 0;
//			int memUnCatedCnt = 0;
			if( !job.outputCollector.isEmpty() ) {
				for(String str:job.outputCollector) {
//					if( str.matches("memCated" + SYMBOL + "\\S+" + SYMBOL + "\\S+" ) ) {
//						memCatedCnt++;
//					} else {
						context.write(new Text(str.trim()), null );		//hdfs	format: memid + SYMBOL + uuid
//						memUnCatedCnt++;
//					}
				}
				job.outputCollector.clear();
			}

			// write personal cated&uncated count to MySQL
//			log.info("mem uncated:" + memUnCatedCnt);
//			log.info("mem cated:" + memCatedCnt);
//			KdclStatisticsSourceDAO dao = new KdclStatisticsSourceDAO();
//			dao.dbInit();
//			dao.deleteByBehaviorAndRecordDate("personal_info", record_date);
//			dao.insert("memid", "member", "personal_info", "Y", memCatedCnt, record_date);		//pcid -> memid		2 -> personal_info
//			dao.insert("memid", "member", "personal_info", "N", memUnCatedCnt, record_date);	//pcid -> memid		2 -> personal_info
//			dao.closeAll();

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

}
