package alex.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;
import com.pchome.soft.depot.utils.KafkaUtil;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class TestRun {

	Log log = LogFactory.getLog(TestRun.class);

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	DateFormatUtil dateFormatUtil;

	
	@Autowired
 	private KafkaUtil KafkaUtil;
	
	private void redisTest() throws Exception{
		
		
		
		
		
//		stg:pa:codecheck:TAC20181210000000001
		
		System.out.println(redisTemplate.opsForValue().get("stg:pa:codecheck:CAC20181210000000001:RLE20190111000000006"));
		
		
		
//		redisTemplate.opsForValue().set("alex", "AAA");
//		redisTemplate.expire("alex", 1, TimeUnit.DAYS);
//		Long a = redisTemplate.getExpire("alex");
//		System.out.println(a);
//		
//		
//		
//		redisTemplate.expire("", timeout, TimeUnit.SECONDS)
		
		
		
		
		
		
//		JSONObject f = new JSONObject();
//		f.put("event", "convert");
////		f.put("event", "tracking");
//		JSONObject c = new JSONObject();
////		c.put("trackId", "traceId002");
//		
//		c.put("convertId", "CAC20181112000000001");
//		c.put("ruleId", "RLE20180724000000001");
//		
//		f.put("codeCondition", c);
//		KafkaUtil.sendMessage("TEST", "TEST-2", "AA");
		
		
		
		
		
		
		
		
//		int partition = 0;
//		String partitionHashcode = "1";
//		for (int i = 0; i < 10000; i++) {
//			KafkaUtil.sendMessage("TEST", partitionHashcode, "thread2_"+i);
//			if(partition == 2){
//				partition = 0;
//				partitionHashcode = "1";
//			}else{
//				partition = partition + 1;
//				if(partition == 1){
//					partitionHashcode = "key0";
//				}
//				if(partition == 2){
//					partitionHashcode = "key2";
//				}
//			}
//		}
	
		
		
//		System.out.println(redisTemplate.opsForValue().get("stg:pa:codecheck:traceId002"));
//		
////		System.out.println(KafkaUtil == null);
//		KafkaUtil.sendMessage("akb_prod_code_check_stg", "", f.toString());
//		String s = (String) redisTemplate.opsForValue().get("Alex");
//		System.out.println(s);
//		
		
//		redisTemplate.
//		System.out.println("1>>>>"+redisTemplate.opsForValue().get("1"));
//		log.info("2>>>>"+redisTemplate.opsForValue().get("1"));
//		String [] data = {"A01","A02","A03","A05","A06","A07","A08","A09","A10","A11","A12","A13","A14","A15","A16","A17","A18","A19",
//				"A20","A21","A22","A23","A24","A25","A26","A27","A28","A29","A30"};
		
//		Date date = new Date(); 
//		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
//		System.out.println(opsForSet.members("nico").size());
//		System.out.println(opsForSet.members("alex").size());
//		redisTemplate.delete("nico");
//		System.out.println(opsForSet.members("nico").size());
		
		
		
//		opsForSet.add("TEST", "CC");
		
//		System.out.println(opsForSet.members("TEST").contains("CC"));
		
//		for (int i = 0; i < 100000; i++) {
//			Random randData = new Random();
//			int no = randData.nextInt(29);
//			String testStr = data[no];	
//			System.out.println(i+":"+testStr);
//			opsForSet.add("nico", testStr);
//		}
		
		
		//1.
//		redisTemplate.opsForSet().add("alex", "A01");
//		 SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		 
		 
//		 String n01 = "N01";
//		 String n02 = "N02";
//		 String n03 = "N03";
//		 String n04 = "A01";
//		 
//		 opsForSet.add("nico", n01);
//		 opsForSet.add("nico", n02);
//		 opsForSet.add("nico", n03);
//		 opsForSet.add("nico", n04);
		 
//		 String a01 = "A01";
//		 String a02 = "A02";
//		 String a03 = "A03";
//		 
//		 opsForSet.add("alex", a01);
//		 opsForSet.add("alex", a02);
//		 opsForSet.add("alex", a03);
		 
		 
//		 System.out.println("nico:"+opsForSet.members("nico") +" size:"+ opsForSet.members("nico").size());
//		 System.out.println("alex:"+opsForSet.members("alex") +" size:"+ opsForSet.members("alex").size());
		 
		 
		 //1.差集
//		 System.out.println("nico,alex 差集: "+opsForSet.difference("nico", "alex")+" size:"+opsForSet.difference("nico", "alex").size());
//		 //2.交集
//		 System.out.println("nico,alex 交集: "+opsForSet.intersect("nico", "alex") +" size:"+opsForSet.intersect("nico", "alex").size());
//		 //3.聯集
//		 System.out.println("nico,alex 聯集: "+opsForSet.union("nico", "alex") +" size:"+opsForSet.union("nico", "alex").size());
//		 
//		 
//		 System.out.println("--------- START TEST- LIST -------------");
//		 ListOperations<String, Object> opsForList = redisTemplate.opsForList();
		 
		 //從右自左發佈消息
//		 opsForList.leftPushAll("bessie", "B01", "B02", "B03");
//		 System.out.println(opsForList.leftPop("bessie"));
		 
		 
//		 List<String> list = new ArrayList<>();
//		 list.add("B01");
//		 list.add("B02");
//		 list.add("B03");
//		 list.add("B04");
//		 redisTemplate.opsForValue().set("bessie", list);
		 
//		 ValueOperations<String, Object> opsForValue = redisTemplate.opsForValue();
//		 System.out.println(opsForValue.get("bessie"));
//		 
//		 
//		 
//		 
//		 HashOperations<String, Object,Object> opsForHash = redisTemplate.opsForHash();
//		 
		 
		 
		 
		 
		 
		 
//		 opsForList.rightPush("bessie",list); 
		 
		 
//		 Long size = opsForList.size("bessie"); 
//		 System.out.println(size);
//		 
//		 
//		 opsForList.leftPop("bessie")
		 
		 
		 
		 
		 
//		System.out.println(redisTemplate.opsForSet().pop("alex"));
		
		
		
//		redisTemplate.opsForSet().add("alex", "A1");
	
//		redisTemplate.opsForValue().set("alex", "123");
		
//		System.out.println(redisTemplate.opsForValue().get("alex"));
		
		
//		redisTemplate.delete("alex");
		//設置自增數字
////		 redisTemplate.opsForValue().increment(key, data);
//		
//		
//		Long a = redisTemplate.opsForValue().increment(key, 5);
//		System.out.println(a);
		
		//寫值
//		redisTemplate.opsForValue().set(key, value, timeOut, TimeUnit.SECONDS);
//		redisTemplate.opsForValue().set(key, value);
//		System.out.println(redisTemplate.getExpire(key));
//		redisTemplate.opsForValue().get(key);
		//取值
//		log.info(">>>>>> "+redisTemplate.opsForValue().get(key));
//		redisTemplate.opsForValue().set(key, value);
//		redisTemplate.expire(key, timeOut, TimeUnit.SECONDS);
//		redisTemplate.delete(key);
	}

	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "stg");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
//		TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
//		TestRun.redisTest();
		
		
//		List<JSONObject> paclJsonInfoList = new ArrayList<JSONObject>();
//		JSONObject json = null;
//		for (int i = 0; i < 2; i++) {
//			JSONObject json2 = new  JSONObject();
//			json2.put("old_alex_"+i, "old_"+i);
//			json = json2;
//			
//			paclJsonInfoList.add(json);
//		}
//		System.out.println(paclJsonInfoList);
//		
		JSONParser jsonParser = new JSONParser();
		
		
		
		String data = "[{'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'10','kdclSourceDate':'2019-01-18 10:25:06','actionSeq':'aa_201901080003','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711060005','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0151','groupSeq':'ag_201901080004','age':'0','fileName':'kwstg1-10.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg2.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-18','adSeq':'ad_201901080002'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'18','kdclSourceDate':'2019-01-22 18:29:32','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711060005','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0134','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-18.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg2.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'18','kdclSourceDate':'2019-01-22 18:25:33','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711040015','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0129','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-18.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'18','kdclSourceDate':'2019-01-22 18:23:44','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711040015','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0129','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-18.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'18','kdclSourceDate':'2019-01-22 18:23:40','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201509250003','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0128','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-18.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'18','kdclSourceDate':'2019-01-22 18:23:36','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201512170007','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0135','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-18.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'09','kdclSourceDate':'2019-01-22 09:47:44','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201510070007','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0124','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-09.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'09','kdclSourceDate':'2019-01-22 09:47:28','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711040015','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0144','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-09.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'09','kdclSourceDate':'2019-01-22 09:47:18','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201711040015','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0144','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-09.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}, {'styleId':'','sex':'','userAgent':'Mozilla-5.0 (Windows NT 6.1; Win64; x64) AppleWebKit-537.36 (KHTML, like Gecko) Chrome-71.0.3578.98 Safari-537.36','priceType':'CPC','categoryCode':'00014000000000000000','pfpCustomerInfoId':'AC2013071700004','keclTime':'09','kdclSourceDate':'2019-01-22 09:47:08','actionSeq':'aa_201901170002','pfbxCustomerInfoId':'PFBC20150519001','pfbxPositionId':'PFBP201509250003','pfdCustomerInfoId':'PFDC20140520001','kdclType':'ck','tproId':'c_x05_pad_tpro_0128','groupSeq':'ag_201901170002','age':'0','fileName':'kwstg1-09.lzo','payType':'1','referer':'http:--showstg.pchome.com.tw-adm-adteststg1.jsp','adType':'2','uuid':'fe06708897d46cdba71d80826f3fae20','kdclDate':'2019-01-22','adSeq':'ad_201901170003'}]";
		
		JSONArray arr =  (JSONArray) jsonParser.parse(data);
		List<JSONObject> list = new ArrayList<>();
		for (Object jsonObject : arr) {
			list.add((JSONObject)jsonObject);
		}

		
		Iterator<JSONObject> iterator = list.iterator();
		while (iterator.hasNext()) {
			JSONObject iteratorJson = (JSONObject)iterator.next();
			iteratorJson.put("alex", "alex");
			if(iteratorJson.getAsString("adSeq").equals("ad_201901080002")){
				iterator.remove();
				continue;
			}
		}
		
		
		for (JSONObject jsonObject : list) {
			System.out.println(jsonObject);
		}
		
		
		
		
		
		
		
		
		
		
		
		
//		List<JSONObject> paclJsonInfoLis2 = new ArrayList<JSONObject>();
//		Map<String,JSONObject> saveDBMap = new HashMap<String,JSONObject>();
//		for (int i = 0; i < 2; i++) {
//			for (JSONObject js : paclJsonInfoList) {
//				JSONObject JSONObject = (net.minidev.json.JSONObject) jsonParser.parse(js.toString());
//				paclJsonInfoLis2.add(JSONObject);
//			}
//			
//			for (JSONObject jsonObject : paclJsonInfoLis2) {
//				jsonObject.put("alex_new_"+i, "alex_"+i);
//			}
//			
//			for (JSONObject jsonObject : paclJsonInfoLis2) {
//				for (Entry<String, Object> jsonObjecte : jsonObject.entrySet()) {
//					saveDBMap.put(jsonObjecte.getKey(), jsonObject);
//				}
//			}
//			
//			System.out.println(paclJsonInfoLis2);
//			
//			paclJsonInfoLis2.clear();
//			
//		}
//		
//		
//		
//		System.out.println(saveDBMap);
//		System.out.println(paclJsonInfoList);
//		
//		
//		
		
		
		
//		StringBuffer g = new StringBuffer();
//		g.append("545454545456454454564545456454  \n dddqd88777897  \n fwe898978\n");
//		g.append("<div class=\" logo-box pos-absolute pos-top pos-left\">");
//		
//		System.out.println(g.toString().indexOf("logo-box pos-absolute pos-top pos-left"));
//		
//		
//		
//		System.out.println(g.toString().substring(74, 112));
		
		
//		System.out.println(g.toString().replace("logo-box pos-absolute pos-top pos-left","type logo-box pos-absolute pos-top pos-left"));
		
		
		
		
		
		
		
////		Connection conn = null;   
////		 try {
////	            Class.forName("com.mysql.jdbc.Driver").newInstance();   //Driver name
////	            String url = "jdbc:mysql://dmpstg.mypchome.com.tw:3306/dmp";    
////	            String user = "webuser";
////	            String password = "7e5nL0H";
////	            conn = DriverManager.getConnection(url, user, password);
////	        } catch (Exception e) {
////	            e.printStackTrace();
////	        }
//		
//		 
//		 
//		 
//		 Connection conn = null;
//	        try
//	        {
//	            //連接MySQL
//	            Class.forName("com.mysql.jdbc.Driver");
//	            //建立讀取資料庫 (test 為資料庫名稱; user 為MySQL使用者名稱; passwrod 為MySQL使用者密碼)
////	        	String url = "jdbc:mysql://dmpadm.mypchome.com.tw:3306/dmp";	//prd
////	        	String url = "jdbc:mysql://dmpstg.mypchome.com.tw:3306/dmp";	//stg
//	            
//	            String url = "jdbc:mysql://dmpstg.mypchome.com.tw:3306/dmp";	//stg
//	        	String user = "webuser";
//	        	String password = "hadoop";
//	        	
////	        	String url = "jdbc:mysql://kdstg.mypchome.com.tw:3306/akb";	//stg
////	        	String user = "keyword";
////	        	String password = "K1y0nLine";
//	        	
//	        	
//	            conn = DriverManager.getConnection(url, user, password);
//	            System.out.println("連接成功MySQL");
////	            Statement st = conn.createStatement();
////	            //撈出剛剛新增的資料
////	            st.execute("SELECT * FROM pcs_prod_category");
////	            ResultSet rs = st.getResultSet();
////	            while(rs.next())
////	            {
////	                System.out.println(rs.getRow());
////	            }
//	        }catch(Exception e)
//	        {
//	        	e.printStackTrace();
//	        }
		 
		 
		 
		 
		 
		
	}
}
