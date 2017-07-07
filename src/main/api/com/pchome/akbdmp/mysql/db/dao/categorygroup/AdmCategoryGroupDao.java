package com.pchome.akbdmp.mysql.db.dao.categorygroup;



import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.akbdmp.mysql.db.dao.base.BaseDAO;



@Repository
public class AdmCategoryGroupDao extends BaseDAO<AdmCategoryGroup, Integer>implements IAdmCategoryGroupDao{

	@Autowired
	SessionFactory sessionFactory;
	
}
