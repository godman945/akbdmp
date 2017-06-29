package com.pchome.hadoopdmp.mongo.job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryAnalyze;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroupAnalyze;
import com.pchome.hadoopdmp.mysql.db.service.categoryanalyze.IAdmCategoryAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categoryanalyze.IAdmCategoryGroupAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categorygroup.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
@Scope("prototype")
public class MapReduceMongoJob {
	
	private static Log log = LogFactory.getLog("MapReduceMongoJob");
	
	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		
		private IAdmCategoryGroupService admCategoryGroupService;
		
		private static Map<String,String> categoryMap = new HashMap<>();
		
		private static int sum = 0;
		
		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admCategoryGroupService = ctx.getBean(IAdmCategoryGroupService.class);
			
				List<AdmCategoryGroup> admGroupList = admCategoryGroupService.loadAll();
				for (AdmCategoryGroup admCategoryGroup : admGroupList) {
					Set<AdmCategory> admCategorySet = admCategoryGroup.getAdmCategories();
					String key = "";
					List<AdmCategory> admCategoryList = new ArrayList<AdmCategory>(admCategorySet);
					int admCategorySize = admCategoryList.size();
					for (AdmCategory admCategory : admCategoryList) {
						if(admCategoryList.indexOf(admCategory) == admCategorySize - 1){
							key = key + admCategory.getAdClass();
						}else{
							key = key + admCategory.getAdClass()+"_";
						}
					}
					if(StringUtils.isNotBlank(key)){
						categoryMap.put(key, admCategoryGroup.getGroupId()+"_TOTAL");
					}
				}
			
				log.info(">>>>>> categoryMap:"+categoryMap);
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
			
		}
		
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			try {
				String update_date = value.get("update_date").toString();
				String category_info_str = value.get("category_info").toString();
				String user_id = value.get("user_id").toString();
				Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
				List<Map<String, Object>> category_info = (List<Map<String, Object>>) value.get("category_info");
				String userType = user_info.get("type").toString();
				Map<String, Set<String>> allMap = new HashMap<String, Set<String>>();
				String mapKey="";

				for (Map<String, Object> category : category_info) {
					String ad_class = category.get("category").toString();
					if(StringUtils.isBlank(ad_class)){
						continue;
					}
					String categoryKey = ad_class + "_" + userType.toUpperCase();
					//(000123_uuid,<"","","">
					context.write(new Text(categoryKey), new Text());

					//process parent
					for (Entry<String, String> entry : categoryMap.entrySet()) {
						if(entry.getKey().indexOf(ad_class) != -1){
							
//							log.info(">>>>>> entry.getKey():"+entry.getKey());
//							log.info(">>>>>> ad_class:"+ad_class);
//							log.info(">>>>>> entry.getValue():"+entry.getValue());
							
							//(000001_TOTAL_UUID,<123,456>)
							if(StringUtils.equals("UUID", userType.toUpperCase())){
								mapKey=entry.getValue()+"_UUID";
							}
							if(StringUtils.equals("MEMID", userType.toUpperCase())){
								mapKey=entry.getValue()+"_MEMID";
							}
							context.write(new Text(mapKey), new Text(user_id));
						}//13840     1.4713 2.9126  = 13839
					}
				}
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private static Set<String> data = new HashSet<>();
		
		private IAdmCategoryGroupAnalyzeService admGroupAnalyzeService;
		
		private IAdmCategoryAnalyzeService admCategoryAnalyzeService;
		
		
		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admGroupAnalyzeService = ctx.getBean(IAdmCategoryGroupAnalyzeService.class);
				admCategoryAnalyzeService = ctx.getBean(IAdmCategoryAnalyzeService.class);
				
			} catch (Exception e) {
				log.error(">>>>> Reducer e : " + e.getMessage());
			}
			
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				data.clear();
				//"0000000000000001_TOTAL"  大分類KEY
				//0015022500000000_uuid		小分類KEY
				if (key.toString().indexOf("TOTAL") > 0) {
					
					String [] array = key.toString().split("_");
					String parentKey = array[0];
					String userType = array[2];
					
					int sum = 0;
					for (Text text : values) {
						data.add(text.toString());
						sum = sum + 1;
					}
					
//					log.info(">>>>> reduce key: " + key);
//					log.info(">>>>> reduce dataSize: " + data.size());
//					log.info(">>>>> reduce sum: " + sum);
					context.write(new Text(parentKey), new Text(String.valueOf(data.size())));
					
					//insert 大分類 mysql
					AdmCategoryGroupAnalyze admCategoryGroupAnalyze = new AdmCategoryGroupAnalyze();
					admCategoryGroupAnalyze.setAdClassCountByHistory(data.size());
					admCategoryGroupAnalyze.setAdGroupId(parentKey);
					admCategoryGroupAnalyze.setUserIdType(userType);
					admCategoryGroupAnalyze.setCreateDate(new Date());
					admGroupAnalyzeService.save(admCategoryGroupAnalyze);					
					
				} else {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
//					log.info(">>>>> reduce key: " + key);
//					log.info(">>>>> reduce sum: " + sum);
//					log.info(">>>>> 小分類: " + key + " : " + sum);
					context.write(key, new Text(String.valueOf(sum)));
					
					//insert 小分類 mysql
					AdmCategoryAnalyze admCategoryAnalyze = new AdmCategoryAnalyze();
					admCategoryAnalyze.setRecodeDate(new Date());
					admCategoryAnalyze.setAdClass(key.toString().split("_")[0]);
					admCategoryAnalyze.setUserIdType(key.toString().split("_")[1]);
					admCategoryAnalyze.setAdClassCountByDay(sum);
					admCategoryAnalyze.setCreateDate(new Date());
					admCategoryAnalyze.setUdpateDate(new Date());
					admCategoryAnalyze.setBehaviorSourceAdClickCount(0);
					admCategoryAnalyze.setBehaviorSource24hCount(0);
					admCategoryAnalyze.setBehaviorSourceRutenCount(0);
					admCategoryAnalyze.setSexManCount(0);
					admCategoryAnalyze.setSexWomanCount(0);
					admCategoryAnalyze.setAgeRangeCount1to10(0);
					admCategoryAnalyze.setAgeRangeCount11to20(0);
					admCategoryAnalyze.setAgeRangeCount21to30(0);
					admCategoryAnalyze.setAgeRangeCount31to40(0);
					admCategoryAnalyze.setAgeRangeCount41to50(0);
					admCategoryAnalyze.setAgeRangeCount51to60(0);
					admCategoryAnalyze.setAgeRangeCount61to70(0);
					admCategoryAnalyze.setAgeRangeCount71to80(0);
					admCategoryAnalyze.setAgeRangeCount81to90(0);
					admCategoryAnalyze.setAgeRangeCount91to100(0);
					admCategoryAnalyzeService.save(admCategoryAnalyze);				
					
				}
			} catch (Exception e) {
				log.error(">>>>> Reducer e : " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count");
		// conf.set("mongo.input.query",
		// "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		// conf.set("mongo.input.query", "{'update_date':{'$gt':'2017-06-19
		// 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);

		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "alex_test");
		Path out = new Path("/home/webuser/alex/mongo");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MapReduceMongoJob.class);
		job.setMapperClass(ReadWeblogsFromMongo.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		new ReadWeblogsFromMongo().setup(null);
	}
}