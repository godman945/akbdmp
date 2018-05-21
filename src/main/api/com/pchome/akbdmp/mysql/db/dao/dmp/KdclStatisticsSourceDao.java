package com.pchome.akbdmp.mysql.db.dao.dmp;



import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.akbdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class KdclStatisticsSourceDao extends BaseDAO<KdclStatisticsSource, Integer>implements IKdclStatisticsSourceDao{
	

	@Autowired
	SessionFactory sessionFactory;
	
	@Override
	public void deleteDmpCountByDate(String recordDate) throws Exception {
		Session session = sessionFactory.getCurrentSession();
		StringBuffer hql = new StringBuffer();
		hql.append("delete from KdclStatisticsSource where recordDate =:recordDate ");
		Query query = session.createQuery(hql.toString());
		query.setParameter("recordDate",recordDate);
		query.executeUpdate();
		session.flush();
	}
}
