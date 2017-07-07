package com.pchome.akbdmp.mysql.db.dao.user;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.AdmMenu;
import com.pchome.akbdmp.mysql.db.dao.base.BaseDAO;


@Repository
public class AdmMenuDao extends BaseDAO<AdmMenu, Integer>implements IAdmMenuDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
