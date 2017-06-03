package com.pchome.soft.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
* 讀取 Property 設定檔資料元件
*/
public class PropertyUtil {
    private static Log log = LogFactory.getLog(PropertyUtil.class);
	private static Properties p = new Properties();
	private static Set<String> pathSet = new HashSet<String>();

	private PropertyUtil() {}

	/**
	 * 清除資料
	 */
	public static void clear() {
	    p = new Properties();
	    pathSet = new HashSet<String>();
	}

    /**
    * 讀取指定設定檔
    * @param path 絕對路徑
    */
	public static void load(String path) {
        pathSet.add(path);

        FileInputStream fis = null;

		try {
		    File file = new File(path);
		    if (!file.exists()) {
		        log.info(path + " not exists");
		        return;
		    }

			fis = new FileInputStream(new File(path));
			p.load(fis);
		}
		catch (IOException e) {
			log.error(path, e);
		}
		finally {
		    if (fis != null) {
		        try {
                    fis.close();
                } catch (IOException e) {
                    log.error(path, e);
                }
		    }
		}
	}

	/**
	 * 重新讀取設定檔
	 */
	public static void reload() {
        p = new Properties();
        for (String path: pathSet) {
            load(path);
        }
	}

	/**
	* 取得 Property 的內容
	* @param key key值
	* @return String value值
	*/
	public static String getProperty(String key) {
		return p.getProperty(key);
	}

	/**
	 * 設定 Property 的內容
	 * @param key
	 * @param value
	 */
	public static void setProperty(String key, String value) {
	    p.setProperty(key, value);
	}
}