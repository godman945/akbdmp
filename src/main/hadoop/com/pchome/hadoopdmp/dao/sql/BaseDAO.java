package com.pchome.hadoopdmp.dao.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;


public class BaseDAO implements IBaseDAO{
   
    protected Log log = LogFactory.getLog(this.getClass());
    
    @Value("${jdbc.driverClass}")
    private static String driverClass;
    
    @Value("${jdbc.driverUrl}")
    private static String driverUrl;
    
    @Value("${jdbc.userName}")
    private static String userName;
    
    @Value("${jdbc.userPassword}")
    private static String userPassword;
    
	/**
	 * 獲得連接
	 * 
	 * @return
	 * @throws Exception
	 */
	public Connection getConn() throws Exception {
		
		 Class.forName(driverClass); 
		 return DriverManager.getConnection(driverUrl, userName, userPassword);

	}

	/**
	 * 關閉連接
	 * 
	 * @param rs
	 * @param sm
	 * @param conn
	 */
	public void closeAll(ResultSet rs, Statement sm, Connection conn)
			throws Exception {
		if (rs != null)
			rs.close();
		if (sm != null)
			sm.close();
		if (conn != null)
			conn.close();
	}

	/**
	 * 增加,刪除,修改
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	public boolean executeUpdate(String sql, Object[] args) {
		boolean flag = false;
		Connection conn = null;
		PreparedStatement sm = null;
		try {
			conn = this.getConn();
			sm = conn.prepareStatement(sql);
			if (conn != null) {
				for (int i = 0; i < args.length; i++) {
					sm.setObject(i + 1, args[i]);
				}
			}
			if (sm.executeUpdate() > 0) {
				flag = true;
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				this.closeAll(null, sm, conn);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 查詢方法
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	public boolean executeQuery(String sql, Object[] args) {
		Connection conn = null;
		PreparedStatement sm = null;
		ResultSet rs = null;
		int count = 0;
		try {
			conn = this.getConn();
			sm = conn.prepareStatement(sql);
			if (args != null) {
				for (int i = 0; i < args.length; i++) {
					sm.setObject(i + 1, args[i]);
				}
			}
			rs = sm.executeQuery();
			if(rs.last()){
				count = rs.getRow();
			}
			log.info("count : " + count);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				this.closeAll(rs, sm, conn);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(count > 0){
			return true;
		}
		return false;
	}
}