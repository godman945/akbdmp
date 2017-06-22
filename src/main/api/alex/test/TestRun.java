//package alex.test;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.stereotype.Component;
//
//import com.pchome.soft.depot.utils.DateFormatUtil;
//
//@Component
//public class TestRun {
//
//	Log log = LogFactory.getLog(TestRun.class);
//
//	@Autowired
//	RedisTemplate<String, Object> redisTemplate;
//
//	@Autowired
//	DateFormatUtil dateFormatUtil;
//
//	private void redisTest() throws Exception{
//		
//		
//		
//		
//		
//		
//		
//		
////		redisTemplate.
////		System.out.println("1>>>>"+redisTemplate.opsForValue().get("1"));
////		log.info("2>>>>"+redisTemplate.opsForValue().get("1"));
////		String [] data = {"A01","A02","A03","A05","A06","A07","A08","A09","A10","A11","A12","A13","A14","A15","A16","A17","A18","A19",
////				"A20","A21","A22","A23","A24","A25","A26","A27","A28","A29","A30"};
//		
////		Date date = new Date(); 
////		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
////		System.out.println(opsForSet.members("nico").size());
////		System.out.println(opsForSet.members("alex").size());
////		redisTemplate.delete("nico");
////		System.out.println(opsForSet.members("nico").size());
//		
//		
//		
////		opsForSet.add("TEST", "CC");
//		
////		System.out.println(opsForSet.members("TEST").contains("CC"));
//		
////		for (int i = 0; i < 100000; i++) {
////			Random randData = new Random();
////			int no = randData.nextInt(29);
////			String testStr = data[no];	
////			System.out.println(i+":"+testStr);
////			opsForSet.add("nico", testStr);
////		}
//		
//		
//		//1.
////		redisTemplate.opsForSet().add("alex", "A01");
////		 SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
//		 
//		 
////		 String n01 = "N01";
////		 String n02 = "N02";
////		 String n03 = "N03";
////		 String n04 = "A01";
////		 
////		 opsForSet.add("nico", n01);
////		 opsForSet.add("nico", n02);
////		 opsForSet.add("nico", n03);
////		 opsForSet.add("nico", n04);
//		 
////		 String a01 = "A01";
////		 String a02 = "A02";
////		 String a03 = "A03";
////		 
////		 opsForSet.add("alex", a01);
////		 opsForSet.add("alex", a02);
////		 opsForSet.add("alex", a03);
//		 
//		 
////		 System.out.println("nico:"+opsForSet.members("nico") +" size:"+ opsForSet.members("nico").size());
////		 System.out.println("alex:"+opsForSet.members("alex") +" size:"+ opsForSet.members("alex").size());
//		 
//		 
//		 //1.差集
////		 System.out.println("nico,alex 差集: "+opsForSet.difference("nico", "alex")+" size:"+opsForSet.difference("nico", "alex").size());
////		 //2.交集
////		 System.out.println("nico,alex 交集: "+opsForSet.intersect("nico", "alex") +" size:"+opsForSet.intersect("nico", "alex").size());
////		 //3.聯集
////		 System.out.println("nico,alex 聯集: "+opsForSet.union("nico", "alex") +" size:"+opsForSet.union("nico", "alex").size());
////		 
////		 
////		 System.out.println("--------- START TEST- LIST -------------");
////		 ListOperations<String, Object> opsForList = redisTemplate.opsForList();
//		 
//		 //從右自左發佈消息
////		 opsForList.leftPushAll("bessie", "B01", "B02", "B03");
////		 System.out.println(opsForList.leftPop("bessie"));
//		 
//		 
////		 List<String> list = new ArrayList<>();
////		 list.add("B01");
////		 list.add("B02");
////		 list.add("B03");
////		 list.add("B04");
////		 redisTemplate.opsForValue().set("bessie", list);
//		 
////		 ValueOperations<String, Object> opsForValue = redisTemplate.opsForValue();
////		 System.out.println(opsForValue.get("bessie"));
////		 
////		 
////		 
////		 
////		 HashOperations<String, Object,Object> opsForHash = redisTemplate.opsForHash();
////		 
//		 
//		 
//		 
//		 
//		 
//		 
////		 opsForList.rightPush("bessie",list); 
//		 
//		 
////		 Long size = opsForList.size("bessie"); 
////		 System.out.println(size);
////		 
////		 
////		 opsForList.leftPop("bessie")
//		 
//		 
//		 
//		 
//		 
////		System.out.println(redisTemplate.opsForSet().pop("alex"));
//		
//		
//		
////		redisTemplate.opsForSet().add("alex", "A1");
//	
////		redisTemplate.opsForValue().set("alex", "123");
//		
////		System.out.println(redisTemplate.opsForValue().get("alex"));
//		
//		
////		redisTemplate.delete("alex");
//		//設置自增數字
//////		 redisTemplate.opsForValue().increment(key, data);
////		
////		
////		Long a = redisTemplate.opsForValue().increment(key, 5);
////		System.out.println(a);
//		
//		//寫值
////		redisTemplate.opsForValue().set(key, value, timeOut, TimeUnit.SECONDS);
////		redisTemplate.opsForValue().set(key, value);
////		System.out.println(redisTemplate.getExpire(key));
////		redisTemplate.opsForValue().get(key);
//		//取值
////		log.info(">>>>>> "+redisTemplate.opsForValue().get(key));
////		redisTemplate.opsForValue().set(key, value);
////		redisTemplate.expire(key, timeOut, TimeUnit.SECONDS);
////		redisTemplate.delete(key);
//	}
//
//	public static void main(String[] args) throws Exception {
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
//		 
//		 
//		 
//		 
//		 
////		System.setProperty("spring.profiles.active", "local");
////		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
////		TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
////		TestRun.redisTest();
//	}
//}
