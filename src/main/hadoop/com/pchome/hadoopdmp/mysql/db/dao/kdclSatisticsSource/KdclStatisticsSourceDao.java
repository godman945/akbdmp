package com.pchome.hadoopdmp.mysql.db.dao.kdclSatisticsSource;


import java.util.List;

import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;

@Repository
public class KdclStatisticsSourceDao extends BaseDAO<KdclStatisticsSource, Integer> implements IKdclStatisticsSourceDao{

	@Override
	public void deleteByBehaviorAndRecordDate(String behavior, String recordDate) throws Exception {
		String hql = "delete from KdclStatisticsSource where recordDate = '" + recordDate + "' and behavior ='"+behavior+"'";
		
		System.out.println(hql);
		
		super.getSessionFactory().getCurrentSession().createQuery(hql.toString()).executeUpdate();
//		session.flush();
	}

	public KdclStatisticsSource findKdclStatisticsSourceByBehaviorAndRecordDate(String behavior, String recordDate,String idType,String classify) throws Exception {
		String hql = " from KdclStatisticsSource where   idType ='"+idType+"'   and  classify ='"+classify+"' and  recordDate = '" + recordDate + "' and behavior ='"+behavior+"'";
		List<KdclStatisticsSource> kdclStatisticsSourceList = super.getSessionFactory().getCurrentSession().createQuery(hql.toString()).list();
		if(kdclStatisticsSourceList.size() > 0){
			return kdclStatisticsSourceList.get(0);
		}
		return  null;
	}
	
	
	@Override
	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insert(String id_type, String service_type, String behavior, String classify, int count,
			String recordDate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	

	

	
}
