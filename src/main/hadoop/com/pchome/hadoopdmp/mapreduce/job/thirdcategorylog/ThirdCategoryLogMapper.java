package com.pchome.hadoopdmp.mapreduce.job.thirdcategorylog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class ThirdCategoryLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("ThirdCategoryLogMapper");
	

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	public JSONParser jsonParser = null;
	public static String record_date;
//	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
//	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
//	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
//	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
//	public static ArrayList<String> prodFileList = new ArrayList<String>();	 								     //24h、ruten第3分類對照表
//	public static ThirdAdClassComponent thirdAdClassComponent = new ThirdAdClassComponent();
//	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
//	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
//	public static DateTimeComponent dateTimeComponent = new DateTimeComponent();
//	public static DeviceComponent deviceComponent = new DeviceComponent();
//	private DB mongoOrgOperations;
//	public static DatabaseReader reader = null;
//	public static InetAddress ipAddress = null;

	@Override
	public void setup(Context context) {
		log.info(">>>>>> Third  Category Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			record_date = context.getConfiguration().get("job.date");
			jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
			Configuration conf = context.getConfiguration();
			
			
			
//			//load 24h、ruten第3分類對照表(ThirdAdClassTable.txt)
//			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
//			Charset charset = Charset.forName("UTF-8");
//			Path thirdAdClassPath = Paths.get(path[6].toString());
//			charset = Charset.forName("UTF-8");
//			List<String> thirdAdClassLines = Files.readAllLines(thirdAdClassPath, charset);
//			for (String line : thirdAdClassLines) {
//				prodFileList.add(line);
//			}
			
			
			
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}

	@Override
	public void map(LongWritable offset, Text mapperValue, Context context) {
		try {
			//讀取kdcl、Campaign資料
			log.info("ThirdCategoryLogMapper raw_data : " + mapperValue.toString());
			
			String data = mapperValue.toString();
			JSONObject jsonObjOrg = (net.minidev.json.JSONObject)jsonParser.parse(data);
			
			JSONObject dataObj =  (JSONObject) jsonObjOrg.get("data");
			JSONArray categoryArray =  (JSONArray) dataObj.get("category_info");
			System.out.println("category_info: "+categoryArray);
			
			JSONArray newCategoryArray = new JSONArray();
			
			for (Object object : categoryArray) {
				JSONObject infoJson = (JSONObject) object;
				String source = infoJson.getAsString("source");
//				String value = infoJson.getAsString("value");
				String url = infoJson.getAsString("url");
//				String day_count = infoJson.getAsString("day_count");
				
				System.out.println("source: "+source);
//				System.out.println("value: "+value);
				System.out.println("url: "+url);
//				System.out.println("day_count: "+day_count);
				
				if ( (!source.equals("24h")) && (!source.equals("ruten")) ) {
					continue;
				}
				
				//如果陣列有24H或ruten資料才處理第3分類
				if (source.equals("24h")) {//確認url符合24h Pattern才塞入array
					Pattern pattern = Pattern.compile("(http|https)://24h.pchome.com.tw/prod/");
					Matcher m = pattern.matcher(url.toString());
					if (m.find()) {
						newCategoryArray.add(object);
					}else{
						continue;
					}
				}
				if (source.equals("ruten")) {//確認url符合ruten Pattern才塞入array
					Pattern pattern = Pattern.compile("(http|https)://goods.ruten.com.tw/item/show+\\?\\d+");
					Matcher m = pattern.matcher(url.toString());
					if (m.find()) {
						newCategoryArray.add(object);
					}else{
						continue;
					}
				}
			}
			
			//如果newCategoryArray有資料，就送入reducer處理第3分類
			JSONObject thirdCategoryObj = new JSONObject();
			if (newCategoryArray.size() > 0){
				JSONObject keyObj = new JSONObject();
				keyObj.put("memid",  ((JSONObject) jsonObjOrg.get("key")).get("memid"));
				keyObj.put("uuid",  ((JSONObject) jsonObjOrg.get("key")).get("uuid"));
				thirdCategoryObj.put("key", keyObj);
				
				JSONObject thirdCategoryDataObj = new JSONObject();
				thirdCategoryDataObj.put("category_info", newCategoryArray);
				thirdCategoryDataObj.put("record_date", jsonObjOrg.get("record_date"));
				thirdCategoryObj.put("data", thirdCategoryDataObj);
				
			}else{
				return;
			}
			
			log.info(">>>>>>ThirdCategoryLogMapper Mapper write key:" + thirdCategoryObj.toString());
			
			keyOut.set(thirdCategoryObj.toString());
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("Third Category Mapper error>>>>>> " +e); 
		}
	}
	
	
	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
	

//	 @Autowired
//	 mongoOrgOperations mongoOrgOperations;
//	 public void test() throws Exception{
//	 System.out.println("BBB");
//	 Query query = new
//	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
//	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
//	 mongoOrgOperations.findOne(query, ClassUrlMongoBean.class);
//	 System.out.println(classUrlMongoBean.getUrl());
//	 System.out.println("AAA");
//	 }

	public static void main(String[] args) throws Exception {
//		 DmpLogMapper dmpLogMapper = new DmpLogMapper();
//		 dmpLogMapper.map(null, null, null);
//
//		System.setProperty("spring.profiles.active", "stg");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		DmpLogMapper dmpLogMapper1 = ctx.getBean(DmpLogMapper.class);
//		
//		dmpLogMapper1.test();
//		dmpLogMapper1.map(null, null, null);

	}
//
}
