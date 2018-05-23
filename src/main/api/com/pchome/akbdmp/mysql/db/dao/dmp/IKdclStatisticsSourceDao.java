package com.pchome.akbdmp.mysql.db.dao.dmp;


import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.akbdmp.mysql.db.dao.base.IBaseDAO;

@Repository
public interface IKdclStatisticsSourceDao extends IBaseDAO<KdclStatisticsSource, Integer>{
	
	public void deleteDmpCountByDate(String recordDate) throws Exception;
}
