package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
@PropertySource({ 
	"classpath:config/prop/${spring.profiles.active}/redis.properties" })
public class TestRun {

	Log log = LogFactory.getLog(TestRun.class);
	
	@Autowired
	TEST test;
	
	@Value("${redis.server}")
	private String server;
	
//	@Autowired
//	MongoOperations mongoOperations;
	
	public void postDocument(){
		
		log.info(">>>>>>>>>>>>>>>>>>");
		System.out.println(test.a());
		
		
//		System.out.println(mongoOperations == null);
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
		TestRun.postDocument();
	}
}
