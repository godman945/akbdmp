package com.pchome.soft.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
public class MysqlUtil {

	private static MysqlUtil singleton = new MysqlUtil();
	
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	Log log = LogFactory.getLog(MysqlUtil.class);

	synchronized static public MysqlUtil getInstance() {
		return singleton;
	}
	
	public void setConnection(String env) throws Exception{
		if(env.equals("prd")){
			
		}else{
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			connect = DriverManager.getConnection(url, user, password);
			connect.setAutoCommit(false); // 设置手动提交 
			statement = connect.createStatement();
		}
	}
	
	public void closeConnection() throws Exception{
		if(connect != null){
			connect.close();
		}
		if(preparedStatement != null){
			preparedStatement.close();
		}
		if(resultSet != null){
			resultSet.close();
		}
		if(statement != null){
			statement.close();
		}
	}
	
	
	public ResultSet query(String sql) throws Exception{
//		log.info("query : "+sql);
		return statement.executeQuery(sql);

	}
	
	public Connection getConnect() {
		return this.connect;
	}


	public static void main(String args[]){
		try {
			
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			MysqlUtil mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection("stg");
			
			
			String jobDate ="2018-12-19";
			
			StringBuffer trackingSql = new StringBuffer();
			trackingSql.append("SELECT ec.catalog_prod_seq ");
			trackingSql.append(" FROM   (SELECT ca.catalog_seq ");
			trackingSql.append(" FROM   (SELECT DISTINCT c.customer_info_id ");
			trackingSql.append(" FROM   pfp_ad_action_report c  ");
			trackingSql.append(" WHERE  c.customer_info_id = (SELECT t.pfp_customer_info_id  ");
			trackingSql.append(" FROM   pfp_code_tracking t  ");
			trackingSql.append(" WHERE  ");
			trackingSql.append(" t.tracking_seq = 'REPLACE_TRACKING')  ");
			trackingSql.append(" AND ad_pvclk_date = '").append(jobDate).append("')c ");
			trackingSql.append(" LEFT JOIN pfp_catalog ca  ");
			trackingSql.append(" ON ca.pfp_customer_info_id = c.customer_info_id  ");
			trackingSql.append(" AND ca.catalog_delete_status = '0'  ");
			trackingSql.append(" AND upload_status = '2')c  ");
			trackingSql.append(" LEFT JOIN pfp_catalog_prod_ec ec  ");
			trackingSql.append(" ON ec.catalog_seq = c.catalog_seq  ");
			trackingSql.append(" WHERE  ec.ec_status = '1'  ");
			trackingSql.append(" AND ec.ec_check_status = '1'  ");
			
			
						
			ResultSet resultSet = mysqlUtil.query(trackingSql.toString().replace("REPLACE_TRACKING", "TAC20181210000000001"));
			while(resultSet.next()){
				String catalog_prod_seq = resultSet.getString("catalog_prod_seq");
				System.out.println(catalog_prod_seq);
			}
			
			
			
			
			
			
			
			
			
			
			
			
//			StringBuffer insertSqlStr = new StringBuffer();
//			insertSqlStr.append(" INSERT INTO `pfp_code_convert_trans`  ");
//			insertSqlStr.append("(uuid,");
//			insertSqlStr.append("convert_date,");
//			insertSqlStr.append("convert_seq,");
//			insertSqlStr.append("convert_trigger_type,");
//			insertSqlStr.append("convert_num_type,");
//			insertSqlStr.append("convert_belong, ");
//			insertSqlStr.append("convert_belong_date,");
//			insertSqlStr.append("convert_count,");
//			insertSqlStr.append("convert_price,");
//			insertSqlStr.append("ad_seq,");
//			insertSqlStr.append("ad_group_seq,");
//			insertSqlStr.append("ad_action_seq,");
//			insertSqlStr.append("ad_type,");
//			insertSqlStr.append("ad_pvclk_date,");
//			insertSqlStr.append("ad_pvclk_time,");
//			insertSqlStr.append("customer_info_id,");
//			insertSqlStr.append("pfbx_customer_info_id,");
//			insertSqlStr.append("pfbx_position_id,");
//			insertSqlStr.append("pfd_customer_info_id,");
//			insertSqlStr.append("pay_type,");
//			insertSqlStr.append("sex,");
//			insertSqlStr.append("age_code,");
//			insertSqlStr.append("time_code,");
//			insertSqlStr.append("template_ad_seq,");
//			insertSqlStr.append("ad_pvclk_website_classify,");
//			insertSqlStr.append("ad_pvclk_audience_classify,");
//			insertSqlStr.append("ad_url,");
//			insertSqlStr.append("style_no,");
//			insertSqlStr.append("ad_pvclk_device,");
//			insertSqlStr.append("ad_pvclk_os,");
//			insertSqlStr.append("ad_pvclk_brand,");
//			insertSqlStr.append("ad_pvclk_area,");
//			insertSqlStr.append("update_date,");
//			insertSqlStr.append("create_date) ");
//			insertSqlStr.append(" VALUES(  ");
//			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
//			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
//			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
//			insertSqlStr.append(" 	?,?,?,? )");
//			
//			System.out.println(insertSqlStr);
//			
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//			Date date = new Date();
//			
//			
//			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
//			String jdbcDriver = "com.mysql.jdbc.Driver";
//			String user = "keyword";
//			String password =  "K1y0nLine";
//			MysqlUtil mysqlUtil = MysqlUtil.getInstance();
//			mysqlUtil.setConnection(url, user, password);
//			
//			
//			
//			PreparedStatement preparedStmt = mysqlUtil.getConnect().prepareStatement(insertSqlStr.toString());
//			preparedStmt.setString(1, "A");
//			preparedStmt.setString(2, sdf.format(date));
//			preparedStmt.setString(3, "convertSeq");
//			preparedStmt.setString(4, "ck");
//			preparedStmt.setString(5, "1");
//			preparedStmt.setString(6, "1");
//			preparedStmt.setString(7, "kdclDate");
//			preparedStmt.setInt(8, Integer.parseInt("1"));
//			preparedStmt.setInt(9,Integer.parseInt("5"));
//			preparedStmt.setString(10,"adSeq");
//			preparedStmt.setString(11,"groupSeq" );
//			preparedStmt.setString(12,"actionSeq");
//			preparedStmt.setString(13,"1") ;
//			preparedStmt.setString(14,"kdclDate" );
//			preparedStmt.setString(15,"08" );
//			preparedStmt.setString(16,"pfpCustomerInfoId" );
//			preparedStmt.setString(17,"pfbxCustomerInfoId" );
//			preparedStmt.setString(18,"pfbxPositionId" );
//			preparedStmt.setString(19,"pfdCustomerInfoId" );
//			preparedStmt.setString(20,"1" );
//			preparedStmt.setString(21,"M" );
//			preparedStmt.setString(22,"ageCode" );
//			preparedStmt.setString(23,"*****" );
//			preparedStmt.setString(24,"*****" );
//			preparedStmt.setString(25,"*****" );
//			preparedStmt.setString(26,"*****" );
//			preparedStmt.setString(27,"referer" );
//			preparedStmt.setString(28,"styleId" );
//			preparedStmt.setString(29,"*****" );
//			preparedStmt.setString(30,"*****" );
//			preparedStmt.setString(31,"*****");
//			preparedStmt.setString(32,"*****");
//			preparedStmt.setDate(33, java.sql.Date.valueOf(sdf.format(date)));
//			preparedStmt.setDate(34,java.sql.Date.valueOf(sdf.format(date)));
			
			
			
			
			
//			System.out.println(mysqlUtil.insert(preparedStmt));
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
//			Log log = LogFactory.getLog(MysqlUtil.class);
//			
//			StringBuffer convertCondition = new StringBuffer();
//			
//			
//			
//			
//			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
//			String jdbcDriver = "com.mysql.jdbc.Driver";
//			String user = "keyword";
//			String password =  "K1y0nLine";
//			MysqlUtil mysqlUtil = MysqlUtil.getInstance();
//			mysqlUtil.setConnection(url, user, password);
//			String convertSeq = "CAC20181112000000001";
//			ResultSet resultSet = mysqlUtil.query(" select * from pfp_code_convert_rule where 1 = 1 and convert_seq = '"+convertSeq+"' ");
//			while(resultSet.next()){
//				String rouleId = resultSet.getString("convert_rule_id");
//				if(resultSet.isLast()){
//					convertCondition.append(rouleId);
//				}else{
//					convertCondition.append(rouleId).append(":");
//				}
////				System.out.println(resultSet.getString("convert_rule_id").length());
////				System.out.println(resultSet.isLast());
////				System.out.println((">>>>>>" + resultSet.getString("convert_rule_id")));
////				System.out.println((">>>>>>" + resultSet.getString("convert_rule_way")));
////				System.out.println((">>>>>>" + resultSet.getString("convert_rule_value")));
//			}
//			
//			List<String> a = new ArrayList<String>();
//			a.add("RLE20180724000000001");
//			a.add("RLE20180724000000002");
//			a.add("RLE20180724000000001");
//			a.add("RLE20180724000000001");
			
			
			
			
			
//			int count = 0;
//			int start = 0;
//			for (String stg : a) {
//				count = 0;
//				start = 0;
//				String sub = stg;
//				while((start = convertCondition.toString().indexOf(sub,start)) >=0){
//		            start += sub.length();
//		            count ++;
//				}
//				System.out.println(stg+":"+count);
//			}
		
			 
			
			
			
//			StringBuffer convertCount = new StringBuffer();
//			for (String stg : a) {
//				
////				if(a.indexOf(convertCondition.toString()) >= 0){
////					
////				}
//				
//				System.out.println(stg);
//				System.out.println(convertCondition.toString());
//				System.out.println(convertCondition.toString().indexOf(stg));
//				
//				
//				break;
//			}
			
			
			
			
//			log.info(">>>>>>>>>convertCondition:"+convertCondition.toString());
//			mysqlUtil.closeConnection();
			
			
//			ResultSet resultSet = MysqlUtil.getInstance().query("select * from pfp_code_convert ");
//			while(resultSet.next()){
//				System.out.println(resultSet.getString("convert_seq"));
//			}
//			
//			
//			resultSet = MysqlUtil.getInstance().query("select * from pfp_code_convert_rule ");
//			while(resultSet.next()){
//				System.out.println(resultSet.getString("convert_rule_id"));
//			}
//			
//			
//			
////			System.out.println(con);
////			
////			
//			MysqlUtil.getInstance().closeConnection();
//			
//			System.out.println(con);
//			
//			con.close();
//			System.out.println(con.isClosed());
//			System.out.println("SSS");
//			con.close();
//			
//			System.out.println(con.isClosed());
//			
//			System.out.println(con);
			
			
			
//			System.out.println(MysqlUtil.getInstance().getConnection(url, user, password));
//			MysqlUtil.getInstance().closeConnection();
//			System.out.println(MysqlUtil.getInstance().getConnection(url, user, password));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

