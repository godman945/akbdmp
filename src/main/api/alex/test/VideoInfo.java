package alex.test;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;


public class VideoInfo {

	public static void main(String[] args) throws Exception {
		String result = "";
		Process process = Runtime.getRuntime().exec(new String[] { "bash", "-c", "ssh vstreamdev youtube-dl -f 18 -g \"https://www.youtube.com/watch?v=3d_eE4T-A-4\"" });
		result = IOUtils.toString(process.getInputStream(),"UTF-8");
		
		JSONObject json = new JSONObject();
		json.put("url", "https://www.youtube.com/watch?v=3d_eE4T-A-4");
		json.put("video_type_18", result);
		System.out.println(json);
		
		
	
		System.out.println("start download....");
		process = Runtime.getRuntime().exec(new String[] { "bash", "-c", "youtube-dl -f 18 https://www.youtube.com/watch?v=3d_eE4T-A-4" });
		result = IOUtils.toString(process.getInputStream(),"UTF-8");
		System.out.println(result);
		
		
	}

}
