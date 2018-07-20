package com.pchome.hadoopdmp.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PfpAdCategoryNewDAO {

	private static Log log = LogFactory.getLog(PfpAdCategoryNewDAO.class);

	Connection conn = null;
	String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb"; // stg
	String jdbcDriver = "com.mysql.jdbc.Driver";
	String user = "keyword";
	String password =  "K1y0nLine";
	Statement stmt=null;
	String sql="";

	public PfpAdCategoryNewDAO() { }
	
	public int querySecondCategoryExist(String code) {
		int parent_id = 0;
		try {
			String sql = " select * from pfp_ad_category_new where 1=1 and code  ='"+code+"' and level =2 ";
			ResultSet rs = stmt.executeQuery(sql);
			log.info("querySecondCategoryExist : "+sql);

			while (rs.next()) {
				parent_id  = rs.getInt("id");
			}
			return parent_id ;

		} catch (Exception e) {
			System.out.println(e);
		}
		return parent_id;
	}
	
	
	public String queryThirdCategoryExist (String code,String name) {
		String level3Code="";
		try {
			String sql = " select * from pfp_ad_category_new where 1=1 and code like '"+code+"%' and name = '"+ name +"' and level =3 ";
			log.info("queryThirdCategoryExist : "+sql);
			
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				level3Code  = rs.getString("code");
			}
			return level3Code ;

		} catch (Exception e) {
			System.out.println(e);
		}
		return code;
	}


	public void insertThirdCategory(String parentId, String code, String name, int level){

		try{
			sql = "insert into `pfp_ad_category_new` ( parent_id, code,name,level,create_date ) values( '"
					+ parentId + "','" +
					code + "','" +
					name + "'," +
					level + "," +
					" NOW()) ";
//					sdf.format(	) + "')";
			log.info("insertThirdCategory : "+sql);
			stmt.executeUpdate(sql);
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}


	public void dbInit(){
		try{
			conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}

	public void closeAll( ) {
		try{
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}



}
