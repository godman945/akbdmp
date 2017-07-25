package com.pchome.hadoopdmp.mysql.db.dao.transferdata;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.DmpTransferDataLog;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;



@Repository
public class DmpTransferDataLogDao extends BaseDAO<DmpTransferDataLog, Integer>implements IDmpTransferDataLogDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
