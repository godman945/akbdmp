package com.pchome.hadoopdmp.mapreduce.job.component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpAddress {
	public static class IpAdd  
    {  
        public boolean isIP(String addr) throws Exception {  
            if(addr.length() < 7 || addr.length() > 15 || "".equals(addr)) {  
                return false;  
            }  
            String rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";  
              
            Pattern pat = Pattern.compile(rexp);    
              
            Matcher mat = pat.matcher(addr);    
              
            boolean ipAddress = mat.find();  
  
            return ipAddress;  
        }  
    }
}