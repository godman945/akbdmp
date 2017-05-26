package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Alex {
	Log log = LogFactory.getLog(Alex.class);
	
	public void alex(){
		log.info("><><><><><><><<>");
	}
	
	public static void main(String[] args) {
		
		Alex Alex = new Alex();
		Alex.alex();
	}

}
