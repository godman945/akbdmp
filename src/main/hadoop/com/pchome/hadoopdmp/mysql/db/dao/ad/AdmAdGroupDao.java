package com.pchome.hadoopdmp.mysql.db.dao.ad;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmAdGroup;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmAdGroupDao extends BaseDAO<AdmAdGroup, Integer>implements IAdmAdGroupDao{
	/**
	 * 回傳PcsUser當日總數作seq序號用
	 * */
	@Autowired
	SessionFactory sessionFactory;
	
//	@SuppressWarnings("unchecked")
//	public List<Object> getPcsUserByKeyword(String pcsUserId,String keyword) throws Exception{
//		StringBuffer sql = new StringBuffer();
//		sql.append(" SELECT a.pcs_user_id,  ");
//		sql.append(" a.login_type,  ");
//		sql.append(" a.pcs_login_account, "); 
//		sql.append(" a.pcs_name, "); 
//		sql.append(" IFNULL(a.pcs_photo_path,'')pcs_photo_path, "); 
//		sql.append(" (SELECT r.pcs_user_id  ");
//		sql.append(" FROM   pcs_user_relation r, "); 
//		sql.append(" pcs_user_relation_mapping m "); 
//		sql.append(" WHERE  r.relation_id = m.relation_id "); 
//		sql.append(" AND m.mapping_id = a.pcs_user_id "); 
//		sql.append(" AND m.status = '1' "); 
//		sql.append(" AND r.pcs_user_id = :pcsUserId )relation_status,");
//		sql.append(" IFNULL(a.pcs_first_name,'')pcs_first_name, "); 
//		sql.append(" IFNULL(a.pcs_last_name,'')pcs_last_name "); 
//		sql.append(" FROM   (SELECT u.pcs_user_id, "); 
//		sql.append(" u.login_type, "); 
//		sql.append(" i.pcs_login_account, "); 
//		sql.append(" u.pcs_name, "); 
//		sql.append(" u.pcs_photo_path, ");
//		sql.append(" u.pcs_first_name, "); 
//		sql.append(" u.pcs_last_name "); 
//		sql.append(" FROM   pcs_user u, "); 
//		sql.append(" pcs_user_login_info i "); 
//		sql.append(" WHERE  u.pcs_user_id = i.pcs_user_id "); 
//		sql.append(" UNION "); 
//		sql.append(" SELECT u.pcs_user_id, "); 
//		sql.append(" u.login_type, "); 
//		sql.append(" f.fb_email, "); 
//		sql.append(" u.pcs_name, "); 
//		sql.append(" u.pcs_photo_path, ");
//		sql.append(" u.pcs_first_name, ");
//		sql.append(" u.pcs_last_name ");
//		sql.append(" FROM   pcs_user_fb_info f, "); 
//		sql.append("  pcs_user u "); 
//		sql.append("  WHERE  u.pcs_bind_fbid = f.fb_id)a "); 
//		if(org.apache.commons.lang.StringUtils.isNotBlank(keyword) && keyword != null){
//			sql.append(" WHERE  a.pcs_login_account LIKE :keyword ");
//			sql.append(" OR a.pcs_name LIKE :keyword "); 
//		}
//		
//		Query query =  sessionFactory.getCurrentSession().createSQLQuery(sql.toString());
//		query.setParameter("pcsUserId", pcsUserId);
//		
//		if(org.apache.commons.lang.StringUtils.isNotBlank(keyword) && keyword != null){
//			query.setParameter("keyword", "%"+keyword+"%");
//			query.setParameter("keyword", "%"+keyword+"%");
//		}
//		return query.list();
//	}
//	
//	public boolean checkPcsUserId(String pcsUserId) throws Exception {
//		
//		StringBuffer hql = new StringBuffer();
//		hql.append("from PcsUser where 1=1");
//		hql.append(" and pcsUserId = ? ");
//
//		Object[] ob = new Object[]{pcsUserId};
//
//		List<PcsUser> list=null;
//		list=super.findHql(hql.toString(),ob);
//		
//		if (list!=null && list.size()==1) {
//			return true;
//		} else {
//			return false;
//		}
//	}
//	
//	public PcsUser getPcsUserByFbid(String fbid) throws Exception {
//		
//		String sql = " from PcsUser where pcsBindFbid = :fbid order by pcsLoginType desc ";
//		Query query =  sessionFactory.getCurrentSession().createQuery(sql);
//		query.setParameter("fbid", fbid);
//		if(query.list().size() == 0){
//			return null;
//		} else {
//			PcsUser pcsUser = null;
//			if(query.list().size() > 1){	//若不只一筆取pcs_login_type='S'那筆
//				for(int i=0;i<query.list().size();i++){
//					PcsUser data = (PcsUser) query.list().get(i);
//					String pcsLoginType = data.getPcsLoginType();
//					if(StringUtils.equals(PcsApiRequestTypeEnum.API_URL_REGISTPCSUSER_LOGIN_DEFAULT_TYPE.getType(), pcsLoginType)){
//						pcsUser = data;
//						break;
//					}
//				}
//			} else {
//				pcsUser = (PcsUser) query.list().get(0);
//			}
//			return pcsUser;
//		}
//	}
//	
//	@SuppressWarnings("unchecked")
//	public List<PcsUser> getPcsUserByFbidList(String fbid) throws Exception{
//		StringBuffer hql = new StringBuffer();
//		hql.append(" from PcsUser where pcsBindFbid = :pcsBindFbid");
//		hql.append(" and loginType = '2' ");
//		Query query =  sessionFactory.getCurrentSession().createQuery(hql.toString());
//		query.setParameter("pcsBindFbid", fbid);
//		return query.list();
//	}
}
