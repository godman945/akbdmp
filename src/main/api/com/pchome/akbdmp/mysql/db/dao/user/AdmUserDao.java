package com.pchome.akbdmp.mysql.db.dao.user;



import java.util.List;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.AdmUser;
import com.pchome.akbdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmUserDao extends BaseDAO<AdmUser, Integer>implements IAdmUserDao{

	@Autowired
	SessionFactory sessionFactory;

	public boolean checkUser(String userId,String password) throws Exception{
		StringBuffer hql = new StringBuffer();
		hql.append("from AdmUser where 1=1");
		hql.append(" and userEmail = ? ");
		hql.append(" and userPassword = ? ");
		Object[] obj = new Object[]{userId,password};
		List<AdmUser> list = super.findHql(hql.toString(),obj);
		
		if (list!=null && list.size() > 0) {
			return true;
		} else {
			return false;
		}
		
	}
}
